const fs = require('fs');
const path = require('path');

function parseDotEnv(content) {
  const result = {};
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) {
      continue;
    }
    const eqIdx = line.indexOf('=');
    if (eqIdx <= 0) {
      continue;
    }
    const key = line.slice(0, eqIdx).trim();
    let value = line.slice(eqIdx + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    result[key] = value;
  }
  return result;
}

function toBool(value, fallback) {
  if (value == null || value === '') {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  return ['1', 'true', 'yes', 'on'].includes(normalized);
}

function stripTrailingSlash(url) {
  return url.replace(/\/+$/, '');
}

const frontendRoot = path.resolve(__dirname, '..');
const repoRoot = path.resolve(frontendRoot, '..');
const envCandidates = [path.join(frontendRoot, '.env'), path.join(repoRoot, '.env')];

let fileEnv = {};
for (const envPath of envCandidates) {
  if (fs.existsSync(envPath)) {
    const content = fs.readFileSync(envPath, 'utf8');
    fileEnv = parseDotEnv(content);
    break;
  }
}

const mergedEnv = { ...fileEnv, ...process.env };

const defaultUseLocalFastApi = !toBool(mergedEnv.CI, false);
const useLocalFastApi = toBool(mergedEnv.USE_LOCAL_FASTAPI, defaultUseLocalFastApi);
const localApiBaseUrl = stripTrailingSlash((mergedEnv.ANGULAR_LOCAL_API_BASE_URL || '').trim());
const onlineApiBaseUrl = stripTrailingSlash(
  (mergedEnv.ANGULAR_ONLINE_API_BASE_URL || 'https://krwvpdmpgw.us-east-1.awsapprunner.com').trim()
);
const apiBaseUrl = useLocalFastApi ? localApiBaseUrl : onlineApiBaseUrl;

const environmentFile = `export const environment = {
  production: false,
  useLocalFastApi: ${useLocalFastApi},
  localApiBaseUrl: '${localApiBaseUrl}',
  onlineApiBaseUrl: '${onlineApiBaseUrl}',
  apiBaseUrl: '${apiBaseUrl}',
};
`;

const outputPath = path.join(frontendRoot, 'src', 'environments', 'environment.ts');
fs.writeFileSync(outputPath, environmentFile, 'utf8');

console.log(
  `[env] Generated environment.ts with USE_LOCAL_FASTAPI=${useLocalFastApi} apiBaseUrl='${apiBaseUrl || '(proxy)'}'`
);
