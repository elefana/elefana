/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.elefana.api.indices;

public enum IndexGenerationMode {
	/**
	 * Generates a PSQL index for all JSON fields
	 */
	ALL,
	/**
	 * Generates a PSQL index for a JSON fields once it is queried at least once
	 */
	DYNAMIC,
	/**
	 * Only generates a PSQL index for pre-specified JSON fields
	 */
	PRESET
}
