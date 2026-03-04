import { describe, it, expect } from 'vitest';
import axios from 'axios';
import crypto from 'crypto';

describe('Binance API Credentials', () => {
  it('should validate Binance API key and secret', async () => {
    const apiKey = process.env.BINANCE_API_KEY;
    const apiSecret = process.env.BINANCE_API_SECRET;

    expect(apiKey).toBeDefined();
    expect(apiSecret).toBeDefined();
    expect(apiKey).toHaveLength(64);
    expect(apiSecret).toHaveLength(64);

    // Test 1: Public endpoint (no auth required)
    const publicResponse = await axios.get('https://api.binance.com/api/v3/ping');
    expect(publicResponse.status).toBe(200);

    // Test 2: Authenticated endpoint - Account information
    const timestamp = Date.now();
    const queryString = `timestamp=${timestamp}`;
    const signature = crypto
      .createHmac('sha256', apiSecret!)
      .update(queryString)
      .digest('hex');

    const authResponse = await axios.get(
      `https://api.binance.com/api/v3/account?${queryString}&signature=${signature}`,
      {
        headers: {
          'X-MBX-APIKEY': apiKey
        }
      }
    );

    expect(authResponse.status).toBe(200);
    expect(authResponse.data).toHaveProperty('balances');
    
    console.log('✅ Binance API credentials validated successfully');
  }, 30000); // 30 second timeout
});
