FROM instructure/rvm

WORKDIR /usr/src/app

RUN /bin/bash -lc "rvm use --default 2.5"

COPY inst-jobs.gemspec Gemfile* /usr/src/app/
COPY lib/delayed/version.rb /usr/src/app/lib/delayed/version.rb
USER root
RUN chown -R docker:docker /usr/src/app
USER docker
RUN /bin/bash -l -c "bundle install"
COPY . /usr/src/app

USER root
RUN chown -R docker:docker /usr/src/app
USER docker

ENV TEST_DB_USERNAME postgres

CMD /bin/bash -l -c "bundle exec wwtd --parallel"
