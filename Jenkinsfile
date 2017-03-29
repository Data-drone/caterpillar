pipeline {
  agent  any
  environment {
    GIT_COMMITTER_NAME = "jenkins"
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
        publishHTML(
          target: [
            allowMissing: false,
            alwaysLinkToLastBuild: true,
            keepAll: true,
            reportDir: 'coverage_report',
            reportFiles: 'index.html',
            reportName: 'Coverage Report'
          ]
        )
      }
    }
  }
  post {
    always {
      deleteDir()
    }
  }
}