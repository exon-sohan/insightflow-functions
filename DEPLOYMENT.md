# Deployment Guide

## GitHub Release Deployment (Recommended for Marketplace)

This is the preferred method for Azure Marketplace applications as it avoids SAS token security issues.

### Step 1: Create GitHub Release

1. Update version in `package.json`
2. Commit and tag:
```bash
git add package.json
git commit -m "Bump version to v1.0.3"
git tag -a v1.0.3 -m "Release v1.0.3"
git push origin master --tags
```

3. Create functions.zip package:
```bash
# Create deployment package
zip -r functions.zip \
  host.json \
  package.json \
  src/ \
  -x "*.git*" "test/*" "*.DS_Store" "node_modules/*"

# Verify package
unzip -l functions.zip
```

4. Create GitHub Release:
- Go to https://github.com/exon-sohan/insightflow-functions/releases
- Click "Create a new release"
- Tag: v1.0.3
- Title: "InsightFlow Functions v1.0.3"
- Description: Release notes
- Upload `functions.zip` as release asset
- Publish release

### Step 2: Update Marketplace Template

Update the ARM template to use the new GitHub release URL:

**File**: `createUiDefinition.json`
```json
{
  "name": "functionsPackageUrl",
  "defaultValue": "https://github.com/exon-sohan/insightflow-functions/releases/download/v1.0.3/functions.zip"
}
```

**Benefits**:
- ✅ No SAS tokens (no CredScan issues)
- ✅ Public URL (no authentication needed)
- ✅ Version controlled releases
- ✅ No expiration dates
- ✅ Free hosting on GitHub

## Azure Blob Storage Deployment (Legacy)

This method requires SAS tokens and should only be used if GitHub releases don't work.

### Upload to Blob Storage

```bash
az storage blob upload \
  --account-name insightflowvendor0494 \
  --container-name releases \
  --name v1.0.3/functions.zip \
  --file functions.zip \
  --auth-mode login
```

### Generate SAS Token

```bash
az storage blob generate-sas \
  --account-name insightflowvendor0494 \
  --container-name releases \
  --name v1.0.3/functions.zip \
  --permissions r \
  --expiry 2027-12-31T23:59:59Z \
  --https-only \
  --output tsv
```

### Use in Template

**Issue**: SAS tokens in ARM templates fail CredScan validation  
**Solution**: Use GitHub releases instead

## Local Development

1. Clone repository:
```bash
git clone https://github.com/exon-sohan/insightflow-functions.git
cd insightflow-functions
```

2. Install dependencies:
```bash
npm install
```

3. Create local.settings.json (copy from README example)

4. Start Functions runtime:
```bash
npm start
```

## Testing Deployment Package

Before creating a release, test the package locally:

```bash
# Create package
zip -r functions.zip host.json package.json src/

# Extract to test location
mkdir /tmp/test-functions
unzip functions.zip -d /tmp/test-functions

# Verify structure
cd /tmp/test-functions
ls -la
cat host.json
cat package.json
ls -la src/functions/

# Test with Functions Core Tools
func start
```

## Deployment URL Comparison

### GitHub Releases (RECOMMENDED)
```
URL: https://github.com/exon-sohan/insightflow-functions/releases/download/v1.0.3/functions.zip
- No authentication required
- No expiration
- Version controlled
- CredScan compliant
- Free hosting
```

### Azure Blob Storage (LEGACY)
```
URL: https://storage.blob.core.windows.net/releases/v1.0.3/functions.zip?sig=ABC...
- Requires SAS token
- Token expires
- CredScan issues
- Costs money
```

## Rollback Procedure

If deployment fails, rollback to previous version:

1. Update createUiDefinition.json to use previous release URL
2. Redeploy marketplace package
3. Existing deployments are unaffected (WEBSITE_RUN_FROM_PACKAGE is set at deployment time)

## Version Management

Semantic versioning: MAJOR.MINOR.PATCH

- **MAJOR**: Breaking changes (e.g., API changes, removed functions)
- **MINOR**: New features (e.g., new functions, new parameters)
- **PATCH**: Bug fixes (e.g., fix sync logic, improve error handling)

Examples:
- v1.0.3 → Bug fix
- v1.1.0 → Added new function
- v2.0.0 → Changed function signatures

## Troubleshooting

### Package Too Large
If zip exceeds GitHub's 2GB limit:
- Remove node_modules (installed at runtime)
- Exclude test files
- Use .funcignore properly

### GitHub Release Not Accessible
Verify URL is correct:
```bash
curl -I "https://github.com/exon-sohan/insightflow-functions/releases/download/v1.0.3/functions.zip"
# Should return: HTTP/2 200
```

### Function App Won't Start
1. Check WEBSITE_RUN_FROM_PACKAGE setting
2. Verify URL is accessible
3. Check Function App logs
4. Verify package structure (must have host.json at root)

## CI/CD (Future Enhancement)

Consider GitHub Actions for automated releases:

```yaml
name: Release Functions
on:
  push:
    tags:
      - 'v*'
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Create package
        run: zip -r functions.zip host.json package.json src/
      - name: Create Release
        uses: softprops/action-gh-release@v1
        with:
          files: functions.zip
```
