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
            values '2.7', '3.0', '3.1', '3.2'
          }
          axis {
            name 'RAILS_VERSION'
            values '6.0', '6.1', '7.0', '7.1'
          }
        }
        excludes {
          exclude {
            axis {
              name 'RUBY_VERSION'
              values '3.2'
            }
            axis {
              name 'RAILS_VERSION'
              values '6.0'
            }
          }
        }
        stages {
          stage('Build') {
            steps {
              timeout(10) {
                // Allow postgres to initialize while the build runs
                sh 'docker-compose up -d postgres'
                sh "docker-compose build --pull --build-arg RUBY_VERSION=${RUBY_VERSION} app"
                sh "BUNDLE_LOCKFILE=activerecord-${RAILS_VERSION} docker-compose run --rm app rspec --tag \\~slow"
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

    stage('Deploy') {
      when {
        allOf {
          expression { GERRIT_BRANCH == "master" }
          environment name: "GERRIT_EVENT_TYPE", value: "change-merged"
        }
      }
      steps {
        lock( // only one build enters the lock
          resource: "${env.JOB_NAME}" // use the job name as lock resource to make the mutual exclusion only for builds from the same branch/tag
        ) {
          withCredentials([string(credentialsId: 'rubygems-rw', variable: 'GEM_HOST_API_KEY')]) {
            sh 'docker-compose run -e GEM_HOST_API_KEY --rm app /bin/bash -lc "./bin/publish.sh"'
          }
        }
      }
    }
  }

  post {
    cleanup {
      sh 'docker-compose down --remove-orphans --rmi all'
    }
  }
}
