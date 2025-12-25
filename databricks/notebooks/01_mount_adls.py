# ============================================================
# Cell 1 — Install Required Library
# ============================================================
# Required for interacting with Azure Data Lake Storage Gen2

%pip install azure-storage-file-datalake

# ============================================================
# Cell 2 — Restart Python Kernel
# ============================================================
# Ensures the installed library is available in the current session

dbutils.library.restartPython()


# ============================================================
# Cell 3 — Azure AD Authentication Configuration
# ============================================================
# Credentials from Azure AD App Registration
# NOTE: Replace placeholders with actual values in Databricks

client_id = "<YOUR_CLIENT_ID>"
tenant_id = "<YOUR_TENANT_ID>"
client_secret = "<YOUR_CLIENT_SECRET>"

# Storage account name
storage_account_name = "<YOUR_ADLS_GEN2_STORAGE_ACCOUNT_NAME>"

# Common config for mounting
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}


# ============================================================
# Cell 4 — Mount ADLS Gen2 Containers
# ============================================================
# Mounts the specified containers to DBFS under /mnt

def mount_container(container_name):
    mount_point = f"/mnt/{container_name}"
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
    mounted = [m.mountPoint for m in dbutils.fs.mounts()]
    
    if mount_point in mounted:
        print(f"{mount_point} is already mounted")
    else:
        try:
            dbutils.fs.mount(
                source=source,
                mount_point=mount_point,
                extra_configs=configs
            )
            print(f"{container_name} mounted successfully at {mount_point}")
        except Exception as e:
            print(f"{container_name} may already be mounted: {e}")

# Mount all required containers
for container in ["raw", "processed", "fraud"]:
    mount_container(container)


# ============================================================
# Cell 5 — Validate Mount Access
# ============================================================
# Writes and deletes a temporary file to confirm read/write access

dbutils.fs.put("/mnt/raw/test.txt", "This is a test file", overwrite=True)
display(dbutils.fs.ls("/mnt/raw"))
dbutils.fs.rm("/mnt/raw/test.txt")
