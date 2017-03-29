pipeline {
  agent  any
  environment {
    GIT_COMMITTER_NAME = "jenkins"
    GIT_COMMITTER_EMAIL = "jenkins@jenkins.io"
  }
  stages {
    stage("Tox") {
      steps {
        sh 'tox -e py27'
      }
    }
    stage("Archive Results"){
      steps {
        junit 'junit*.xml'
      }
    }
  }
  post {
    always {
      deleteDir()
    }
  }
}