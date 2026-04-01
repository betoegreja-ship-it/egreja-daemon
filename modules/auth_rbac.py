"""
Email-based RBAC module for Egreja Investment AI v10.22

Self-contained role-based access control system with:
- Role-based authorization (VIEWER, OPERATOR, ADMIN)
- User management with email-based authentication
- API key generation and validation
- Immutable audit logging
- Thread-safe operations

No imports from api_server.py.
"""

import os
import logging
import hashlib
import secrets
import threading
from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Tuple, List, Callable, Any
from functools import wraps


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Role(Enum):
    """Role enumeration for RBAC."""
    VIEWER = "viewer"        # Can see status, /ops, trades; no modifications
    OPERATOR = "operator"    # Can toggle kill switch, close trades, adjust parameters
    ADMIN = "admin"          # Full access: users, deploy, config


class User(dict):
    """User model - dict-based for MySQL storage."""

    def __init__(self, email: str, role: Role, created_by: str,
                 is_active: bool = True, api_key_hash: Optional[str] = None,
                 created_at: Optional[datetime] = None,
                 last_access: Optional[datetime] = None):
        """
        Initialize a User.

        Args:
            email: User email (primary key)
            role: Role enum value
            created_by: Email of admin who created this user
            is_active: Whether user account is active
            api_key_hash: SHA256 hash of user's API key
            created_at: User creation timestamp
            last_access: Last access timestamp
        """
        super().__init__()
        self['email'] = email
        self['role'] = role.value if isinstance(role, Role) else role
        self['created_by'] = created_by
        self['is_active'] = is_active
        self['api_key_hash'] = api_key_hash
        self['created_at'] = created_at or datetime.utcnow()
        self['last_access'] = last_access or datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to plain dict for JSON serialization."""
        return dict(self)


class AuditLogger:
    """Immutable audit log manager."""

    # Allowed action types
    ACTIONS = {
        'LOGIN', 'LOGOUT', 'TRADE_OPEN', 'TRADE_CLOSE',
        'KILL_SWITCH', 'CONFIG_CHANGE', 'USER_ADD', 'USER_REMOVE',
        'DEPLOY', 'AUTH_FAILED', 'UNAUTHORIZED_ACCESS'
    }

    @staticmethod
    def log_action(email: str, action: str, detail: str, ip_address: str,
                   get_db_func: Callable) -> bool:
        """
        Log an action immutably to audit_log table.

        Args:
            email: User email performing action
            action: Action type from ACTIONS set
            detail: Action details (JSON string or text)
            ip_address: IP address of request
            get_db_func: Callable that returns db connection

        Returns:
            True if logged successfully, False otherwise
        """
        if action not in AuditLogger.ACTIONS:
            logger.warning(f"Invalid audit action: {action}")
            return False

        try:
            db = get_db_func()
            cursor = db.cursor()

            sql = """
                INSERT INTO audit_log (ts, email, action, detail, ip_address)
                VALUES (%s, %s, %s, %s, %s)
            """

            cursor.execute(sql, (
                datetime.utcnow(),
                email,
                action,
                detail,
                ip_address
            ))

            db.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Audit log failed: {e}")
            return False

    @staticmethod
    def get_recent(limit: int = 100, get_db_func: Optional[Callable] = None) -> List[Dict]:
        """
        Retrieve recent audit log entries.

        Args:
            limit: Maximum number of entries to return
            get_db_func: Callable that returns db connection

        Returns:
            List of audit log dicts, newest first
        """
        if not get_db_func:
            return []

        try:
            db = get_db_func()
            cursor = db.cursor(dictionary=True)

            sql = """
                SELECT id, ts, email, action, detail, ip_address
                FROM audit_log
                ORDER BY ts DESC
                LIMIT %s
            """

            cursor.execute(sql, (limit,))
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"Audit log retrieval failed: {e}")
            return []


class AuthManager:
    """
    Central authentication and authorization manager.
    Thread-safe with internal locking.
    """

    def __init__(self, admin_email: Optional[str] = None,
                 auth_mode: Optional[str] = None, api_secret_key: Optional[str] = None):
        """
        Initialize AuthManager.

        Args:
            admin_email: Email of admin user (default from env ADMIN_EMAIL,
                        fallback 'betoegreja@hotmail.com')
            auth_mode: Authentication mode - 'api_key' or 'email_token'
                      (default from env AUTH_MODE, fallback 'api_key')
            api_secret_key: Secret key for initial admin API key
        """
        self.admin_email = admin_email or os.getenv('ADMIN_EMAIL', 'betoegreja@hotmail.com')
        self.auth_mode = auth_mode or os.getenv('AUTH_MODE', 'api_key')
        self.api_secret_key = api_secret_key
        self._lock = threading.RLock()

        logger.info(f"AuthManager initialized - Admin: {self.admin_email}, Mode: {self.auth_mode}")

    def init_users_table(self, get_db_func: Callable) -> bool:
        """
        Create rbac_users table if not exists, and ensure admin user exists.

        Args:
            get_db_func: Callable that returns db connection

        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            try:
                db = get_db_func()
                cursor = db.cursor()

                # Create rbac_users table
                users_table_sql = """
                    CREATE TABLE IF NOT EXISTS rbac_users (
                        email VARCHAR(255) PRIMARY KEY,
                        role VARCHAR(50) NOT NULL,
                        created_by VARCHAR(255) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_access TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE,
                        api_key_hash VARCHAR(255),
                        INDEX idx_role (role),
                        INDEX idx_active (is_active)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                cursor.execute(users_table_sql)

                # Create audit_log table
                audit_table_sql = """
                    CREATE TABLE IF NOT EXISTS audit_log (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        email VARCHAR(255) NOT NULL,
                        action VARCHAR(50) NOT NULL,
                        detail TEXT,
                        ip_address VARCHAR(45),
                        INDEX idx_email (email),
                        INDEX idx_action (action),
                        INDEX idx_ts (ts)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                cursor.execute(audit_table_sql)

                db.commit()

                # Ensure admin exists
                cursor.execute("SELECT * FROM rbac_users WHERE email = %s", (self.admin_email,))
                admin_exists = cursor.fetchone()

                if not admin_exists:
                    # Generate API key hash for admin from secret
                    api_key_hash = None
                    if self.api_secret_key:
                        api_key_hash = hashlib.sha256(
                            self.api_secret_key.encode()
                        ).hexdigest()

                    insert_sql = """
                        INSERT INTO rbac_users
                        (email, role, created_by, api_key_hash, is_active)
                        VALUES (%s, %s, %s, %s, TRUE)
                    """
                    cursor.execute(insert_sql, (
                        self.admin_email,
                        Role.ADMIN.value,
                        self.admin_email,
                        api_key_hash
                    ))
                    db.commit()
                    logger.info(f"Admin user created: {self.admin_email}")

                cursor.close()
                return True
            except Exception as e:
                logger.error(f"Failed to initialize users table: {e}")
                return False

    def _hash_key(self, key: str) -> str:
        """Hash an API key using SHA256."""
        return hashlib.sha256(key.encode()).hexdigest()

    def authenticate(self, api_key_or_token: str, get_db_func: Callable) -> Optional[User]:
        """
        Authenticate user by API key or token.

        Args:
            api_key_or_token: API key or email token
            get_db_func: Callable that returns db connection

        Returns:
            User dict if authenticated, None otherwise
        """
        with self._lock:
            try:
                db = get_db_func()
                cursor = db.cursor(dictionary=True)

                if self.auth_mode == 'api_key':
                    key_hash = self._hash_key(api_key_or_token)
                    sql = """
                        SELECT * FROM rbac_users
                        WHERE api_key_hash = %s AND is_active = TRUE
                    """
                    cursor.execute(sql, (key_hash,))
                else:
                    # email_token mode: token is just the email
                    sql = """
                        SELECT * FROM rbac_users
                        WHERE email = %s AND is_active = TRUE
                    """
                    cursor.execute(sql, (api_key_or_token,))

                user_row = cursor.fetchone()
                cursor.close()

                if user_row:
                    # Update last_access
                    cursor = db.cursor()
                    cursor.execute(
                        "UPDATE rbac_users SET last_access = %s WHERE email = %s",
                        (datetime.utcnow(), user_row['email'])
                    )
                    db.commit()
                    cursor.close()

                    # Convert to User object
                    user = User(
                        email=user_row['email'],
                        role=Role(user_row['role']),
                        created_by=user_row['created_by'],
                        is_active=user_row['is_active'],
                        api_key_hash=user_row['api_key_hash'],
                        created_at=user_row['created_at'],
                        last_access=user_row['last_access']
                    )
                    return user

                return None
            except Exception as e:
                logger.error(f"Authentication failed: {e}")
                return None

    def authorize(self, user: User, required_role: Role) -> bool:
        """
        Check if user has sufficient role for action.

        Args:
            user: User object
            required_role: Required role

        Returns:
            True if authorized, False otherwise
        """
        if not user:
            return False

        user_role = Role(user['role']) if isinstance(user['role'], str) else user['role']

        # Role hierarchy: VIEWER < OPERATOR < ADMIN
        role_hierarchy = {
            Role.VIEWER: 0,
            Role.OPERATOR: 1,
            Role.ADMIN: 2
        }

        user_level = role_hierarchy.get(user_role, -1)
        required_level = role_hierarchy.get(required_role, -1)

        return user_level >= required_level

    def add_user(self, email: str, role: Role, created_by: str,
                 get_db_func: Callable) -> Tuple[bool, str]:
        """
        Add a new user (ADMIN only).

        Args:
            email: New user email
            role: Role to assign
            created_by: Email of admin adding user
            get_db_func: Callable that returns db connection

        Returns:
            Tuple of (success, message)
        """
        with self._lock:
            try:
                db = get_db_func()
                cursor = db.cursor(dictionary=True)

                # Verify creator is admin
                cursor.execute(
                    "SELECT role FROM rbac_users WHERE email = %s",
                    (created_by,)
                )
                creator = cursor.fetchone()

                if not creator or creator['role'] != Role.ADMIN.value:
                    return False, "Only ADMIN can add users"

                # Check if user exists
                cursor.execute("SELECT email FROM rbac_users WHERE email = %s", (email,))
                if cursor.fetchone():
                    return False, f"User {email} already exists"

                # Add user
                sql = """
                    INSERT INTO rbac_users (email, role, created_by, is_active)
                    VALUES (%s, %s, %s, TRUE)
                """
                cursor.execute(sql, (email, role.value, created_by))
                db.commit()
                cursor.close()

                logger.info(f"User added: {email} ({role.value}) by {created_by}")
                return True, f"User {email} added successfully"
            except Exception as e:
                logger.error(f"Add user failed: {e}")
                return False, f"Error adding user: {str(e)}"

    def remove_user(self, email: str, removed_by: str,
                    get_db_func: Callable) -> Tuple[bool, str]:
        """
        Remove a user (ADMIN only, cannot remove self).

        Args:
            email: Email of user to remove
            removed_by: Email of admin removing user
            get_db_func: Callable that returns db connection

        Returns:
            Tuple of (success, message)
        """
        with self._lock:
            try:
                if email == removed_by:
                    return False, "Cannot remove yourself"

                db = get_db_func()
                cursor = db.cursor(dictionary=True)

                # Verify remover is admin
                cursor.execute(
                    "SELECT role FROM rbac_users WHERE email = %s",
                    (removed_by,)
                )
                remover = cursor.fetchone()

                if not remover or remover['role'] != Role.ADMIN.value:
                    return False, "Only ADMIN can remove users"

                # Check if user exists
                cursor.execute("SELECT email FROM rbac_users WHERE email = %s", (email,))
                if not cursor.fetchone():
                    return False, f"User {email} not found"

                # Delete user
                cursor.execute("DELETE FROM rbac_users WHERE email = %s", (email,))
                db.commit()
                cursor.close()

                logger.info(f"User removed: {email} by {removed_by}")
                return True, f"User {email} removed successfully"
            except Exception as e:
                logger.error(f"Remove user failed: {e}")
                return False, f"Error removing user: {str(e)}"

    def update_role(self, email: str, new_role: Role, updated_by: str,
                    get_db_func: Callable) -> Tuple[bool, str]:
        """
        Update user role (ADMIN only).

        Args:
            email: User email
            new_role: New role
            updated_by: Email of admin updating role
            get_db_func: Callable that returns db connection

        Returns:
            Tuple of (success, message)
        """
        with self._lock:
            try:
                db = get_db_func()
                cursor = db.cursor(dictionary=True)

                # Verify updater is admin
                cursor.execute(
                    "SELECT role FROM rbac_users WHERE email = %s",
                    (updated_by,)
                )
                updater = cursor.fetchone()

                if not updater or updater['role'] != Role.ADMIN.value:
                    return False, "Only ADMIN can update roles"

                # Check if target user exists
                cursor.execute("SELECT email FROM rbac_users WHERE email = %s", (email,))
                if not cursor.fetchone():
                    return False, f"User {email} not found"

                # Update role
                sql = "UPDATE rbac_users SET role = %s WHERE email = %s"
                cursor.execute(sql, (new_role.value, email))
                db.commit()
                cursor.close()

                logger.info(f"User role updated: {email} -> {new_role.value} by {updated_by}")
                return True, f"User {email} role updated to {new_role.value}"
            except Exception as e:
                logger.error(f"Update role failed: {e}")
                return False, f"Error updating role: {str(e)}"

    def list_users(self, get_db_func: Callable) -> List[Dict]:
        """
        List all users.

        Args:
            get_db_func: Callable that returns db connection

        Returns:
            List of user dicts
        """
        try:
            db = get_db_func()
            cursor = db.cursor(dictionary=True)

            sql = "SELECT email, role, created_by, created_at, last_access, is_active FROM rbac_users ORDER BY created_at DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"List users failed: {e}")
            return []

    def generate_api_key(self, email: str, get_db_func: Callable) -> Optional[str]:
        """
        Generate new API key for user and store hash.

        Args:
            email: User email
            get_db_func: Callable that returns db connection

        Returns:
            New API key (plain text, one-time) or None if failed
        """
        with self._lock:
            try:
                # Generate random key
                api_key = secrets.token_urlsafe(32)
                key_hash = self._hash_key(api_key)

                db = get_db_func()
                cursor = db.cursor()

                # Update key hash for user
                cursor.execute(
                    "UPDATE rbac_users SET api_key_hash = %s WHERE email = %s",
                    (key_hash, email)
                )
                db.commit()
                cursor.close()

                logger.info(f"API key generated for: {email}")
                return api_key
            except Exception as e:
                logger.error(f"Generate API key failed: {e}")
                return None

    def get_user_by_email(self, email: str, get_db_func: Callable) -> Optional[User]:
        """
        Retrieve user by email.

        Args:
            email: User email
            get_db_func: Callable that returns db connection

        Returns:
            User object or None if not found
        """
        try:
            db = get_db_func()
            cursor = db.cursor(dictionary=True)

            cursor.execute("SELECT * FROM rbac_users WHERE email = %s", (email,))
            user_row = cursor.fetchone()
            cursor.close()

            if user_row:
                user = User(
                    email=user_row['email'],
                    role=Role(user_row['role']),
                    created_by=user_row['created_by'],
                    is_active=user_row['is_active'],
                    api_key_hash=user_row['api_key_hash'],
                    created_at=user_row['created_at'],
                    last_access=user_row['last_access']
                )
                return user

            return None
        except Exception as e:
            logger.error(f"Get user by email failed: {e}")
            return None


def require_role(required_role: Role):
    """
    Decorator for route handlers requiring specific role.

    Returns authorization decorator function.

    Usage:
        @require_role(Role.ADMIN)
        def admin_endpoint(request, auth_manager, get_db_func):
            ...

    The decorated function receives:
    - request: Flask request object
    - auth_manager: AuthManager instance
    - get_db_func: Database connection callable

    Returns:
        401 Unauthorized if no API key in X-API-Key header
        403 Forbidden if user lacks required role
        Otherwise calls handler with (request, auth_manager, get_db_func)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, request=None, auth_manager=None, get_db_func=None, **kwargs):
            """
            Wrapper that enforces authentication and authorization.

            Args:
                request: Flask request object with headers
                auth_manager: AuthManager instance
                get_db_func: Callable returning db connection

            Returns:
                403 dict if insufficient role
                401 dict if no authentication
                Otherwise: result of decorated function
            """
            if not request or not auth_manager or not get_db_func:
                return {'error': 'Missing required parameters'}, 400

            # Extract API key from header
            api_key = request.headers.get('X-API-Key')

            if not api_key:
                return {'error': 'Unauthorized - no API key'}, 401

            # Authenticate
            user = auth_manager.authenticate(api_key, get_db_func)

            if not user:
                ip = request.remote_addr or 'unknown'
                AuditLogger.log_action(
                    'unknown',
                    'AUTH_FAILED',
                    f'Invalid API key from {ip}',
                    ip,
                    get_db_func
                )
                return {'error': 'Unauthorized - invalid credentials'}, 401

            # Authorize
            if not auth_manager.authorize(user, required_role):
                ip = request.remote_addr or 'unknown'
                AuditLogger.log_action(
                    user['email'],
                    'UNAUTHORIZED_ACCESS',
                    f'Attempted {func.__name__} without {required_role.value} role',
                    ip,
                    get_db_func
                )
                return {'error': f'Forbidden - requires {required_role.value} role'}, 403

            # Call handler
            return func(*args, request=request, user=user,
                       auth_manager=auth_manager, get_db_func=get_db_func, **kwargs)

        return wrapper
    return decorator
