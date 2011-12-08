(ns com.camptocamp.aws.sqs
  (:use [com.camptocamp.aws :only (clojurize doto-if javaize)])
  (:import [com.amazonaws.services.sqs AmazonSQSClient])
  (:import [com.amazonaws.services.sqs.model
            CreateQueueRequest
            DeleteMessageRequest
            DeleteQueueRequest
            GetQueueAttributesRequest
            ListQueuesRequest
            SendMessageRequest
            ReceiveMessageRequest]))

(defn client [credentials region]
  (doto (AmazonSQSClient. credentials)
    (.setEndpoint region)))

(defn create-queue [client
                     queue-name
                     & {:keys [default-visibility-timeout]}]
  (let [request (doto-if (CreateQueueRequest. (name queue-name))
                  (.setDefaultVisibilityTimeout default-visibility-timeout))]
    {:client client
     :url    (.getQueueUrl (.createQueue client request))}))

(defn delete-queue
  ([queue]
    (delete-queue (:client queue) (:url queue)))
  ([client queue-url]
    (let [request (DeleteQueueRequest. queue-url)]
      (.deleteQueue client request))))

(defn get-all-queues [client
                      & {:keys [queue-name-prefix]}]
  (let [request (ListQueuesRequest.)
        request (if queue-name-prefix
                  (.withQueueNamePrefix (name queue-name-prefix))
                  request)]
    (map #(hash-map :client client :url %)
         (.getQueueUrls (.listQueues client request)))))

(def queue-attribute-value-parsers
  {:approximate-number-of-messages #(java.lang.Integer/parseInt %)
   :approximate-number-of-messages-not-visible #(java.lang.Integer/parseInt %)
   :created-timestamp #(java.lang.Integer/parseInt %)
   :last-modified-timestamp #(java.lang.Integer/parseInt %)
   :maximum-message-size #(java.lang.Integer/parseInt %)
   :message-retention-period #(java.lang.Integer/parseInt %)
   :visibility-timeout #(java.lang.Integer/parseInt %)})

(defn parse-queue-attribute-value [k v]
  ((get queue-attribute-value-parsers k identity) v))

(defn get-queue-attributes [queue & attribute-names]
  (let [request (doto (GetQueueAttributesRequest. (:url queue))
                  (.setAttributeNames (map javaize (or attribute-names [:all]))))
        attributes (.getAttributes (.getQueueAttributes (:client queue) request))]
    (into {}
          (map (fn [[k v]]
                (let [k (clojurize k)]
                  [k (parse-queue-attribute-value k v)]))
               attributes))))

(defn get-queue-attribute [queue attribute]
  (attribute (get-queue-attributes queue attribute)))

(defn receive-messages [queue
                        & {:keys [attribute-names
                                  max-number-of-messages
                                  visibility-timeout]}]
  (let [request (doto-if (ReceiveMessageRequest. (:url queue))
                  (.setAttributeNames attribute-names)
                  (.setMaxNumberOfMessages max-number-of-messages)
                  (.setVisibilityTimeout visibility-timeout))]
    (map #(hash-map :message % :queue queue)
         (-> (:client queue) (.receiveMessage request) .getMessages))))

(defn receive-message [queue
                       & {:keys [attribute-names
                                 visibility-timeout]}]
  (first (receive-messages queue
                          :attribute-names attribute-names
                          :visibility-timeout visibility-timeout)))

(defn message-body [message]
  (-> message :message .getBody))

(defn message-receipt-handle [message]
  (-> message :message .getReceiptHandle))

(defn delete-message-by-receipt-handle [queue message-receipt-handle]
  (let [request (DeleteMessageRequest. (:url queue) message-receipt-handle)]
    (.deleteMessage (:client queue) request)))

(defn delete-message [message]
  (delete-message-by-receipt-handle (:queue message)
                                    (message-receipt-handle message)))

(defn send-message [queue message-body]
  (let [request (SendMessageRequest. (:url queue) message-body)]
   (.sendMessage (:client queue) request)))
