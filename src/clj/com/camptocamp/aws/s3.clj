(ns com.camptocamp.aws.s3
  (:use [com.camptocamp.aws :only (doto-if)])
  (:import [com.amazonaws.services.s3 AmazonS3Client])
  (:import [com.amazonaws.services.s3.model
            DeleteObjectsRequest
            DeleteObjectsRequest$KeyVersion
            ListObjectsRequest
            S3ObjectSummary]))

(defn client [credentials]
  (AmazonS3Client. credentials))

(defn list-buckets [client]
  (.listBuckets client))

(defn lazy-object-listing [client objectListing]
  (cons
    objectListing
    (if (.isTruncated objectListing)
      (lazy-seq (lazy-object-listing client (.listNextBatchOfObjects client objectListing)))
      nil)))

(defn list-objects [client bucket-name & {:keys [delimiter marker max-keys prefix]}]
  (let [request (doto-if (ListObjectsRequest.)
                  (.setBucketName bucket-name)
                  (.setDelimiter delimiter)
                  (.setMarker marker)
                  (.setMaxKeys max-keys)
                  (.setPrefix prefix))
        objectListing (.listObjects client request)]
    (mapcat #(.getObjectSummaries %) (lazy-object-listing client objectListing))))

(defmulti get-key (fn [object] (class object)))
(defmethod get-key S3ObjectSummary [object] (.getKey object))
(defmethod get-key :default [object] object)

(defn delete-object [client bucket-name object]
  (.deleteObject client bucket-name (get-key object)))

(defn delete-objects [client bucket-name objects & {:keys [mfa quiet]}]
  (doseq [objects (partition-all 1000 objects)]
    (let [keys (map #(DeleteObjectsRequest$KeyVersion. (get-key %)) objects)
          request (doto-if (DeleteObjectsRequest. bucket-name)
                    (.setKeys keys)
                    (.setMfa mfa)
                    (.setQuiet quiet))]
      (.deleteObjects client request))))

(defn delete-objects-with-prefix [client bucket-name prefix]
  (let [objects (list-objects client bucket-name :prefix prefix)]
    (delete-objects client bucket-name objects)))
