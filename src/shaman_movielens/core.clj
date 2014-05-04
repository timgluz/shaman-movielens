(ns shaman-movielens.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [shaman.core :as shaman]))

(defn store-user
  [client [user-id age gender occupation zip-code]]
  (shaman/add-user client
                   user-id
                   {:pio_inactive false
                    :age age
                    :gender gender
                    :occupation gender
                    :zip-code zip-code}))

(defn store-movie
  [client features]
  (let [item-values (take 5 features)
        item-titles [:movie-id :title :release :video-release :imdb]
        genre-values (drop 6 features) ;skips unknown field
        genre-names [:action :adventure :animation :children :comedy
                     :crime :documentary :drama :fantasy :film-noir
                     :horror :musical :mystery :romance :scifi
                     :thriller :war :western]]
    (shaman/add-item client
                     (first item-values)
                     ["movie" "movielens" "100k"]
                     (merge {}
                            (zipmap item-titles item-values)
                            (zipmap genre-names genre-values)))))

(defn store-action
  [client [user-id movie-id rating timestamp]]
  (shaman/add-action client user-id movie-id :rate rating))

(defn preprocess-row
  "splits value string into components because
    every row-item in ml-100k dataset is formated as a|b|c\tabd|e|f"
  [row]
  (string/split (first row) #"\|"))

(defn foreach-row
  "applies side-effect `map-fn` for every row and
   closes input reader soon after it finished iteration."
  [map-fn filepath]
  (with-open [in-file (io/reader filepath)]
    (doseq [row (csv/read-csv in-file :separator \tab)]
      (map-fn row))))

(comment
  ;; HAMMOCK1: recommend 10 movies to user
  (require '[shaman-movielens.core :refer :all] :reload)
  (require '[shaman.core :as shaman])

  (def api-key "pK4cwVhBEMk7IWRTpXd869X5EUjKC9vNeISURqaRaZnXlpWnAeVTlJxWkZTZlkD0")
  (def host "http://10.0.10.2")
  (def client (shaman/make-client host api-key))

  ;;import all users
  (foreach-row
    (fn [row]
      (store-user client (preprocess-row row)))
    "resources/ml-100k/u.user")

  (shaman/get-user client "1")
  (shaman/get-user client "100")

  ;;import items
  (foreach-row
    (fn [row]
      (store-movie client (preprocess-row row)))
    "resources/ml-100k/u.item")

  (shaman/get-item client "1")
  (shaman/get-item client "42")

  ;; add actions
  (foreach-row
    (partial store-action client)
    "resources/ml-100k/u.data")

  ;; get predictions
  ;; it expects that you have already trained model
  ;; otherwise you will see message "Cannot find recommendation"
  (shaman/recommend-topn client "movielens-rec" "1" 10)

) ;; end of comment block
