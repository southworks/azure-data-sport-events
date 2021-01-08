using Azure.Core;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using System;


namespace Helpers.KeyVault
{
    public class AzureKeyVaultHelper
    {
        public string GetSecret(string key, string keyVaultName)
        {
            SecretClientOptions options = new SecretClientOptions()
            {
                Retry =
                {
                    Delay= TimeSpan.FromSeconds(2),
                    MaxDelay = TimeSpan.FromSeconds(16),
                    MaxRetries = 5,
                    Mode = RetryMode.Exponential
                 }
            };
            var getKeyVaultUrl = BuildKeyVaultUrl(keyVaultName);
            var client = new SecretClient(new Uri(getKeyVaultUrl), new DefaultAzureCredential(new DefaultAzureCredentialOptions { ExcludeSharedTokenCacheCredential = true }), options);
            KeyVaultSecret saccountConnection = client.GetSecret(key);
            return saccountConnection.Value;
        }
        
        private string BuildKeyVaultUrl (string keyVaultName)
        {
            return "https://" + keyVaultName + ".vault.azure.net/";
        }
    }
}