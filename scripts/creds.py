from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()
#
# Verify
print(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
