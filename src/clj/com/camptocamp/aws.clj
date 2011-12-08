(ns com.camptocamp.aws
  (:import [com.amazonaws.auth BasicAWSCredentials])
  (:require [clojure.string :as string]))

(defn credentials [^String access-key-id ^String secret-access-key]
  (BasicAWSCredentials. access-key-id secret-access-key))

(defn credentials-from-environment []
  (credentials (System/getenv "AWS_ACCESS_KEY_ID")
               (System/getenv "AWS_SECRET_ACCESS_KEY")))

(defn clojurize [^String s]
  (keyword (string/join "-" (map string/lower-case (re-seq #"[A-Z][a-z]+|[A-Z]" s)))))

(defn javaize [^Keyword k]
  (apply str (map string/capitalize (string/split (name k) #"-"))))

(defmacro doto-if [x & forms]
  (let [gx (gensym)]
    `(let [~gx ~x]
       ~@(map (fn [[f arg & args]]
                (let [garg (gensym)]
                  `(let [~garg ~arg]
                     (if ~garg
                       (~f ~gx ~garg ~@args)))))
              forms)
       ~gx)))
