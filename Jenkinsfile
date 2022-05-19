#! /usr/bin/env groovy

pipeline {
  agent { label 'docker' }

  environment {
    // Make sure we're ignoring any override files that may be present
    COMPOSE_FILE = "docker-compose.yml"
  }

  stages {
    stage('Test') {
      matrix {
        agent { label 'docker' }
        axes {
          axis {
            name 'RUBY_VERSION'
            values '2.6', '2.7', '3.0'
          }
          axis {
            name 'RAILS_VERSION'
            values '6.0', '6.1', '7.0'
          }
        }
        excludes {
          exclude {
            axis {
              name 'RUBY_VERSION'
              values '2.6'
            }
            axis {
              name 'RAILS_VERSION'
              values '7.0'
            }
          }
        }
        stages {
          stage('Build') {
            steps {
              timeout(10) {
                // Allow postgres to initialize while the build runs
                sh 'docker-compose up -d postgres'
                sh "docker-compose build --pull --build-arg RUBY_VERSION=${RUBY_VERSION} --build-arg BUNDLE_GEMFILE=gemfiles/activerecord_${RAILS_VERSION}.gemfile app"
                sh 'docker-compose run --rm app rspec --tag \\~slow'
              }
            }
          }
        }
      }
    }

    stage('Lint') {
      steps {
        sh "docker-compose build --pull"
        sh "docker-compose run --rm app bin/rubocop"
      }
    }
  }

  post {
    cleanup {
      sh 'docker-compose down --remove-orphans --rmi all'
    }
  }
}
