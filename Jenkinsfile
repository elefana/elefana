pipeline {
    agent none
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
					mail (to: "${params.emailAddress}",
						subject: "elefana Build ${env.BUILD_NUMBER} failed",
						body: "Please go to ${env.BUILD_URL}.")
				}
			}
		}
    }
}
