def version = 'alpha35'

pipeline {
    agent none
    environment {
        BUILD_EMAIL_ADDRESS     = credentials('build-email-address')
    }
    stages {
    	stage('Linux Build') {
			agent {
				label 'linux'
			}
			steps {
				sh './gradlew clean build'
			}
			post {
				always {
					junit '**/build/test-results/test/*.xml'
				}
				failure {
					mail (to: $BUILD_EMAIL_ADDRESS,
						subject: "elefana Build ${env.BUILD_NUMBER} failed",
						body: "Please go to ${env.BUILD_URL}.")
				}
			}
		}
    }
}
