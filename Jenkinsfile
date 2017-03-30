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
            ),
            // Handle a weird edgecase in environment variable handling for twine
            // This affects the jenkins user in particular, because there is no 
            // .pypirc for them.
            // https://github.com/pypa/twine/issues/206
            string(
              credentialsId: 'devpi_test_index_url',
              variable: 'TWINE_REPOSITORY'
            )
          ]) {
          sh 'python setup.py sdist'
          sh 'python -m twine upload dist/*'
        }
      }
    }
    stage("Test Downstream"){
      when {
        branch 'master'
      }
      steps {
        build job: '/Kapiche/caterpillar-influence/master', wait: false
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