const { execSync } = require('child_process');

// Usar tRPC para verificar dados
const checkData = `
curl -s http://localhost:3000/api/trpc/sofia.getTrades | jq '.result.data' | head -20
`;

console.log('Verificando trades via tRPC...');
try {
  const result = execSync(checkData, { encoding: 'utf-8' });
  console.log(result);
} catch (error) {
  console.error('Erro ao verificar:', error.message);
}
