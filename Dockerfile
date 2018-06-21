# For Development
# Invoke for development with:
# docker build -t nyc-data-processing .
# docker run -it --rm --name nyc-data-repl -v $(pwd):/app nyc-data-processing lein repl
FROM clojure:lein-2.8.1-alpine
MAINTAINER @brycemcd <bryce@bridgetownint.com>

RUN mkdir /app

WORKDIR /app

CMD lein repl