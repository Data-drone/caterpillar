pipeline {
  agent  any
  environment {
    GIT_COMMITTER_NAME = "jenkins"
  }
  stages {
    stage("Test") {
      steps {
        sh 'tox'
      }
    }
    stage("Archive Package"){
      when {
          branch 'master'
      }
      steps {
        withCredentials(
          [
            usernamePassword(
              credentialsId: '3cf688b7-5905-44a4-865a-58dc4ce900ff',
              passwordVariable: 'TWINE_PASSWORD',
              usernameVariable: 'TWINE_USERNAME'
            ),
            string(
              credentialsId: 'devpi_test_index_url',
              variable: 'TWINE_REPOSITORY_URL'
            )
          ]) {
          sh 'python setup.py sdist bdist_wheel'
          sh 'twine upload dist/*'
        }
      }
    }
  }
  post {
    always {
      junit 'junit*.xml'
      publishHTML(
        target: [
          allowMissing: false,
          alwaysLinkToLastBuild: false,
          keepAll: true,
          reportDir: 'coverage_report',
          reportFiles: 'index.html',
          reportName: 'Coverage Report'
        ]
      )
      deleteDir()
    }
  }
}