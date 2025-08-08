// Copyright (C) 2025, The Duplicati Team
// https://duplicati.com, hello@duplicati.com
// 
// Permission is hereby granted, free of charge, to any person obtaining a 
// copy of this software and associated documentation files (the "Software"), 
// to deal in the Software without restriction, including without limitation 
// the rights to use, copy, modify, merge, publish, distribute, sublicense, 
// and/or sell copies of the Software, and to permit persons to whom the 
// Software is furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in 
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
// DEALINGS IN THE SOFTWARE.

using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Duplicati.Library.Common.IO;
using Duplicati.Library.Interface;
using Duplicati.Library.Utility;
using Duplicati.Library.Utility.Options;
using System.Runtime.CompilerServices;

namespace Duplicati.Library.Backend
{
    /// <summary>
    /// Azure Blob Storage client that implements the IS3Client interface for compatibility
    /// </summary>
    public class AzureBlobClient : IS3Client
    {
        /// <summary>
        /// The maximum number of items to list in a single request
        /// </summary>
        private const int ITEM_LIST_LIMIT = 1000;

        /// <summary>
        /// The prefix for extended options
        /// </summary>
        private const string EXT_OPTION_PREFIX = "azure-ext-";

        /// <summary>
        /// The storage tier for blobs
        /// </summary>
        private readonly AccessTier? m_accessTier;
        /// <summary>
        /// The Blob service client
        /// </summary>
        private readonly BlobServiceClient m_client;

        /// <summary>
        /// The DNS host of the Azure Blob Storage endpoint
        /// </summary>
        private readonly string? m_dnsHost;
        /// <summary>
        /// The timeouts to use
        /// </summary>
        private readonly TimeoutOptionsHelper.Timeouts m_timeouts;

        /// <summary>
        /// The archive classes that are considered archive classes
        /// </summary>
        private readonly IReadOnlySet<AccessTier> m_archiveClasses;

        /// <summary>
        /// The option to specify the archive classes
        /// </summary>
        public const string AZURE_ARCHIVE_CLASSES_OPTION = "azure-archive-classes";

        /// <summary>
        /// The default storage classes that are considered archive classes
        /// </summary>
        public static readonly IReadOnlySet<AccessTier> DEFAULT_ARCHIVE_CLASSES = new HashSet<AccessTier>([
            AccessTier.Cold, AccessTier.Archive
        ]);

        public AzureBlobClient(string accountName, string? accessKey, string? sasToken, string? accessTier, 
            TimeoutOptionsHelper.Timeouts timeouts, Dictionary<string, string?> options)
        {
            if (sasToken != null)
            {
                var sasUri = new System.Uri($"https://{accountName}.blob.core.windows.net/?{sasToken}");
                m_client = new BlobServiceClient(sasUri);
            }
            else if (!string.IsNullOrEmpty(accessKey))
            {
                var connectionString = $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accessKey};EndpointSuffix=core.windows.net";
                m_client = new BlobServiceClient(connectionString);
            }
            else
            {
                throw new ArgumentException("Either accessKey or sasToken must be provided");
            }

            m_timeouts = timeouts;
            m_accessTier = string.IsNullOrWhiteSpace(accessTier) ? null : new AccessTier(accessTier);
            m_dnsHost = m_client.Uri.Host;
            m_archiveClasses = ParseStorageClasses(options.GetValueOrDefault(AZURE_ARCHIVE_CLASSES_OPTION));
        }

        /// <summary>
        /// Parses the storage classes from the string
        /// </summary>
        /// <param name="storageClass">The storage class string</param>
        /// <returns>The storage classes</returns>
        private static IReadOnlySet<AccessTier> ParseStorageClasses(string? storageClass)
        {
            if (string.IsNullOrWhiteSpace(storageClass))
                return DEFAULT_ARCHIVE_CLASSES;

            return new HashSet<AccessTier>(storageClass.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(x => new AccessTier(x)));
        }

        /// <inheritdoc/>
        public Task AddBucketAsync(string bucketName, CancellationToken cancelToken)
        {
            var containerClient = m_client.GetBlobContainerClient(bucketName);
            return Utility.Utility.WithTimeout(m_timeouts.ShortTimeout, cancelToken, ct => containerClient.CreateAsync(PublicAccessType.None, cancellationToken: ct));
        }

        public virtual async Task GetFileStreamAsync(string bucketName, string keyName, Stream target, CancellationToken cancelToken)
        {
            try
            {
                var containerClient = m_client.GetBlobContainerClient(bucketName);
                var blobClient = containerClient.GetBlobClient(keyName);

                using (var timeoutStream = target.ObserveWriteTimeout(m_timeouts.ReadWriteTimeout, false))
                    await Utility.Utility.WithTimeout(m_timeouts.ShortTimeout, cancelToken, ct => blobClient.DownloadToAsync(timeoutStream, ct)).ConfigureAwait(false);
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                throw new FileMissingException($"File {keyName} not found", ex);
            }
        }

        public string? GetDnsHost()
        {
            return m_dnsHost;
        }

        public virtual async Task AddFileStreamAsync(string bucketName, string keyName, Stream source,
            CancellationToken cancelToken)
        {
            var containerClient = m_client.GetBlobContainerClient(bucketName);
            var blobClient = containerClient.GetBlobClient(keyName);

            using var timeoutStream = source.ObserveReadTimeout(m_timeouts.ReadWriteTimeout, false);
            
            var options = new BlobUploadOptions()
            {
                Conditions = null, // Overwrite any existing blob
                AccessTier = m_accessTier
            };

            try
            {
                await Utility.Utility.WithTimeout(m_timeouts.ShortTimeout, cancelToken, ct => blobClient.UploadAsync(timeoutStream, options, ct)).ConfigureAwait(false);
            }
            catch (RequestFailedException e) when (e.Status == 404)
            {
                throw new FolderMissingException(e);
            }
        }

        public Task DeleteObjectAsync(string bucketName, string keyName, CancellationToken cancellationToken)
        {
            var containerClient = m_client.GetBlobContainerClient(bucketName);
            var blobClient = containerClient.GetBlobClient(keyName);

            return Utility.Utility.WithTimeout(m_timeouts.ShortTimeout, cancellationToken, ct => blobClient.DeleteIfExistsAsync(cancellationToken: ct));
        }

        /// <summary>
        /// Lists the contents of a container
        /// </summary>
        /// <param name="bucketName">The container to list</param>
        /// <param name="prefix">The prefix to list</param>
        /// <param name="recursive">If true, the list is recursive</param>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <returns>The list of files</returns>
        public async IAsyncEnumerable<IFileEntry> ListBucketAsync(string bucketName, string prefix, bool recursive, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var containerClient = m_client.GetBlobContainerClient(bucketName);

            var traits = BlobTraits.None;
            var states = BlobStates.None;

            await using var blobEnumerator = containerClient.GetBlobsAsync(traits, states, prefix, cancellationToken).GetAsyncEnumerator(cancellationToken);
            
            while (true)
            {
                bool hasNext;

                try
                {
                    hasNext = await Utility.Utility.WithTimeout(m_timeouts.ListTimeout, cancellationToken, async ct =>
                        await blobEnumerator.MoveNextAsync().ConfigureAwait(false)
                    ).ConfigureAwait(false);
                }
                catch (RequestFailedException ex) when (ex.Status == 404)
                {
                    throw new FolderMissingException(ex);
                }

                if (!hasNext) break;

                cancellationToken.ThrowIfCancellationRequested();

                if (blobEnumerator.Current is { } blob)
                {
                    var blobName = System.Uri.UnescapeDataString(blob.Name.Replace("+", "%2B"));

                    // Skip if not matching prefix (additional safety check)
                    if (!string.IsNullOrEmpty(prefix) && !blobName.StartsWith(prefix))
                        continue;

                    // Handle recursive vs non-recursive listing
                    if (!recursive && !string.IsNullOrEmpty(prefix))
                    {
                        var relativePath = blobName.Substring(prefix.Length);
                        if (relativePath.Contains('/'))
                        {
                            // This is a subdirectory, skip if not recursive
                            continue;
                        }
                    }

                    var lastModified = blob.Properties.LastModified?.UtcDateTime ?? DateTime.UtcNow;
                    var isArchive = blob.Properties.AccessTier.HasValue && m_archiveClasses.Contains(blob.Properties.AccessTier.Value);

                    // Remove prefix from the name for the result
                    var displayName = string.IsNullOrEmpty(prefix) ? blobName : blobName.Substring(prefix.Length);

                    yield return new FileEntry(
                        displayName,
                        blob.Properties.ContentLength ?? 0,
                        lastModified,
                        lastModified
                    )
                    {
                        IsArchived = isArchive,
                        IsFolder = false,
                    };
                }
            }
        }

        public async Task RenameFileAsync(string bucketName, string source, string target, CancellationToken cancelToken)
        {
            var containerClient = m_client.GetBlobContainerClient(bucketName);
            var sourceBlobClient = containerClient.GetBlobClient(source);
            var targetBlobClient = containerClient.GetBlobClient(target);

            // Copy source to target
            var copyOperation = await Utility.Utility.WithTimeout(m_timeouts.ListTimeout, cancelToken, ct => targetBlobClient.StartCopyFromUriAsync(sourceBlobClient.Uri, cancellationToken: ct)).ConfigureAwait(false);
            
            // Wait for copy to complete
            await copyOperation.WaitForCompletionAsync(cancelToken).ConfigureAwait(false);
            
            // Delete original
            await DeleteObjectAsync(bucketName, source, cancelToken).ConfigureAwait(false);
        }

        #region IDisposable Members

        public void Dispose()
        {
            // BlobServiceClient doesn't need explicit disposal
        }

        #endregion
    }
}
