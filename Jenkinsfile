def version = 'alpha35'

pipeline {
    agent none
    stages {
    	stage('Linux Build') {
    	    environment {
                BUILD_EMAIL_ADDRESS     = credentials('build-email-address')
            }
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
