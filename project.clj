(defproject io.mandoline/mandoline-dynamodb "0.1.10-SNAPSHOT"
  :description "DynamoDB backend for Mandoline."
  :license {:name "Apache License, version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :min-lein-version "2.0.0"
  :url
    "https://github.com/TheClimateCorporation/mandoline-dynamodb"
  :mailing-lists
    [{:name "mandoline-users@googlegroups.com"
      :archive "https://groups.google.com/d/forum/mandoline-users"
      :post "mandoline-users@googlegroups.com"}
     {:name "mandoline-dev@googlegroups.com"
      :archive "https://groups.google.com/d/forum/mandoline-dev"
      :post "mandoline-dev@googlegroups.com"}]

  :checksum :warn
  :dependencies
    [[org.clojure/clojure "1.7.0"]
     [org.slf4j/slf4j-log4j12 "1.7.12"]
     [log4j "1.2.17"]
     [org.clojure/tools.logging "0.3.1"]
     [org.clojure/core.cache "0.6.4"]
     [org.clojure/core.memoize "0.5.7"]
     [joda-time/joda-time "2.8.1"]
     [com.amazonaws/aws-java-sdk "1.10.5.1"]
     [com.taoensso/faraday "1.7.1"]
     [io.mandoline/mandoline-core "0.1.10"]]
  :exclusions [org.clojure/clojure]

  :aliases {"docs" ["marg" "-d" "target"]
            "package" ["do" "clean," "jar"]}

  :plugins [[lein-ancient "0.6.7"]
            [lein-cloverage "1.0.2"]
            [lein-marginalia "0.7.1"]]
  :test-selectors {:default :unit
                   :integration :integration
                   :local :local
                   :unit :unit
                   :all (constantly true)})
