#!/bin/bash
# Microsoft Graph Delegated Token Acquisition Script
# This script helps acquire OAuth2 tokens for testing Graph API functions
# that require delegated (user) permissions.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
PORT=8080
REDIRECT_URI="http://localhost:${PORT}/callback"

# Check for required environment variables
if [ -z "$ERPL_MS_TENANT_ID" ]; then
    echo -e "${RED}Error: ERPL_MS_TENANT_ID environment variable is required${NC}"
    echo "Run: source .env.microsoft"
    exit 1
fi

if [ -z "$ERPL_MS_CLIENT_ID" ]; then
    echo -e "${RED}Error: ERPL_MS_CLIENT_ID environment variable is required${NC}"
    exit 1
fi

# Scopes for delegated access (user permissions)
# These are the permissions needed for Teams, Outlook, and OneDrive
SCOPES="openid profile offline_access User.Read Files.Read.All Mail.Read Calendars.Read Contacts.Read Team.ReadBasic.All Channel.ReadBasic.All"

# URL encode the scopes
ENCODED_SCOPES=$(echo "$SCOPES" | sed 's/ /%20/g')

# Generate PKCE code verifier (43-128 characters, RFC 7636)
CODE_VERIFIER=$(openssl rand -base64 64 | tr -d '\n' | tr '+/' '-_' | tr -d '=' | cut -c1-64)

# Generate code challenge (S256 method)
CODE_CHALLENGE=$(echo -n "$CODE_VERIFIER" | openssl dgst -sha256 -binary | base64 | tr '+/' '-_' | tr -d '=')

# Generate state for CSRF protection
STATE=$(openssl rand -hex 16)

# Build authorization URL
AUTH_URL="https://login.microsoftonline.com/${ERPL_MS_TENANT_ID}/oauth2/v2.0/authorize"
AUTH_URL="${AUTH_URL}?client_id=${ERPL_MS_CLIENT_ID}"
AUTH_URL="${AUTH_URL}&response_type=code"
AUTH_URL="${AUTH_URL}&redirect_uri=${REDIRECT_URI}"
AUTH_URL="${AUTH_URL}&response_mode=query"
AUTH_URL="${AUTH_URL}&scope=${ENCODED_SCOPES}"
AUTH_URL="${AUTH_URL}&state=${STATE}"
AUTH_URL="${AUTH_URL}&code_challenge=${CODE_CHALLENGE}"
AUTH_URL="${AUTH_URL}&code_challenge_method=S256"

echo -e "${GREEN}=== Microsoft Graph Delegated Token Acquisition ===${NC}"
echo ""
echo -e "${YELLOW}Step 1: Open the following URL in your browser to sign in:${NC}"
echo ""
echo "$AUTH_URL"
echo ""

# Try to open browser automatically
if command -v open &> /dev/null; then
    echo "Opening browser..."
    open "$AUTH_URL"
elif command -v xdg-open &> /dev/null; then
    echo "Opening browser..."
    xdg-open "$AUTH_URL"
fi

echo ""
echo -e "${YELLOW}Step 2: After signing in, you'll be redirected to localhost:${PORT}${NC}"
echo "The page will show an error (that's expected)."
echo ""
echo -e "${YELLOW}Step 3: Copy the FULL URL from your browser's address bar and paste it here:${NC}"
echo ""
read -p "Callback URL: " CALLBACK_URL

# Extract authorization code from callback URL
if [[ "$CALLBACK_URL" =~ code=([^&]+) ]]; then
    AUTH_CODE="${BASH_REMATCH[1]}"
else
    echo -e "${RED}Error: Could not extract authorization code from URL${NC}"
    exit 1
fi

# Verify state matches
if [[ "$CALLBACK_URL" =~ state=([^&]+) ]]; then
    RETURNED_STATE="${BASH_REMATCH[1]}"
    if [ "$RETURNED_STATE" != "$STATE" ]; then
        echo -e "${RED}Error: State mismatch - possible CSRF attack${NC}"
        exit 1
    fi
fi

echo ""
echo -e "${GREEN}Authorization code received. Exchanging for tokens...${NC}"

# Exchange authorization code for tokens
TOKEN_URL="https://login.microsoftonline.com/${ERPL_MS_TENANT_ID}/oauth2/v2.0/token"

RESPONSE=$(curl -s -X POST "$TOKEN_URL" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "client_id=${ERPL_MS_CLIENT_ID}" \
    -d "scope=${SCOPES}" \
    -d "code=${AUTH_CODE}" \
    -d "redirect_uri=${REDIRECT_URI}" \
    -d "grant_type=authorization_code" \
    -d "code_verifier=${CODE_VERIFIER}")

# Check for error
if echo "$RESPONSE" | grep -q '"error"'; then
    echo -e "${RED}Error exchanging code for tokens:${NC}"
    echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    exit 1
fi

# Extract tokens
ACCESS_TOKEN=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null)
REFRESH_TOKEN=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin).get('refresh_token', ''))" 2>/dev/null)
EXPIRES_IN=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin).get('expires_in', 3600))" 2>/dev/null)

# Calculate expiration timestamp
EXPIRES_AT=$(($(date +%s) + EXPIRES_IN))

echo ""
echo -e "${GREEN}=== Tokens acquired successfully! ===${NC}"
echo ""
echo "Add the following to your .env.microsoft file:"
echo ""
echo -e "${YELLOW}# Delegated (user) access token for Teams, Outlook, OneDrive${NC}"
echo "export ERPL_MS_DELEGATED_ACCESS_TOKEN=\"${ACCESS_TOKEN}\""
echo "export ERPL_MS_DELEGATED_REFRESH_TOKEN=\"${REFRESH_TOKEN}\""
echo "export ERPL_MS_DELEGATED_EXPIRES_AT=\"${EXPIRES_AT}\""
echo ""
echo -e "${YELLOW}Token expires at: $(date -r $EXPIRES_AT 2>/dev/null || date -d @$EXPIRES_AT 2>/dev/null)${NC}"
echo ""
echo "To use in tests, create a secret with:"
echo ""
echo "CREATE SECRET ms_graph_delegated ("
echo "    TYPE microsoft_graph,"
echo "    PROVIDER config,"
echo "    tenant_id '\${ERPL_MS_TENANT_ID}',"
echo "    client_id '\${ERPL_MS_CLIENT_ID}',"
echo "    access_token '\${ERPL_MS_DELEGATED_ACCESS_TOKEN}',"
echo "    refresh_token '\${ERPL_MS_DELEGATED_REFRESH_TOKEN}',"
echo "    expires_at '\${ERPL_MS_DELEGATED_EXPIRES_AT}'"
echo ");"
