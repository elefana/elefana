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
package com.elefana.search;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class PsqlQueryComponents {
	private String fromComponent;
	private String whereComponent;
	private String groupByComponent;
	private String orderByComponent;
	private String limitComponent;
	private final List<String> temporaryTables;

	public PsqlQueryComponents(String fromComponent, String whereComponent, String groupByComponent, String orderByComponent) {
		this(fromComponent, whereComponent, groupByComponent, orderByComponent, "");
	}

	public PsqlQueryComponents(String fromComponent, String whereComponent, String groupByComponent, String orderByComponent,
			String limitComponent) {
		this(fromComponent, whereComponent, groupByComponent, orderByComponent, limitComponent, new ArrayList<String>(1));
	}

	public PsqlQueryComponents(PsqlQueryComponents queryComponents) {
		this(new String(queryComponents.fromComponent), new String(queryComponents.whereComponent), new String(queryComponents.groupByComponent),
				new String(queryComponents.orderByComponent), new String(queryComponents.limitComponent),
				new ArrayList<String>(queryComponents.temporaryTables));
	}

	public PsqlQueryComponents(String fromComponent, String whereComponent, String groupByComponent, String orderByComponent,
			String limitComponent, List<String> temporaryTables) {
		super();
		this.fromComponent = fromComponent;
		this.whereComponent = whereComponent;
		this.groupByComponent = groupByComponent;
		this.orderByComponent = orderByComponent;
		this.limitComponent = limitComponent;
		this.temporaryTables = temporaryTables;
	}
	
	public boolean appendWhere(StringBuilder queryBuilder) {
		if(whereComponent.isEmpty()) {
			return false;
		}
		queryBuilder.append(" WHERE ");
		queryBuilder.append(whereComponent);
		return true;
	}
	
	public boolean appendGroupBy(StringBuilder queryBuilder) {
		if(groupByComponent.isEmpty()) {
			return false;
		}
		queryBuilder.append(" GROUP BY ");
		queryBuilder.append(groupByComponent);
		return true;
	}
	
	public boolean appendOrderBy(StringBuilder queryBuilder) {
		if(orderByComponent.isEmpty()) {
			return false;
		}
		queryBuilder.append(" ORDER BY ");
		queryBuilder.append(orderByComponent);
		return true;
	}
	
	public boolean appendLimit(StringBuilder queryBuilder) {
		if(limitComponent.isEmpty()) {
			return false;
		}
		queryBuilder.append(limitComponent);
		return true;
	}

	public PsqlQueryComponents andWhere(String clause) {
		whereComponent += " AND (" + clause + ")";
		return this;
	}

	public PsqlQueryComponents orWhere(String clause) {
		whereComponent += " OR (" + clause + ")";
		return this;
	}

	public String getFromComponent() {
		return fromComponent;
	}

	public String getWhereComponent() {
		return whereComponent;
	}

	public String getGroupByComponent() {
		return groupByComponent;
	}

	public String getOrderByComponent() {
		return orderByComponent;
	}

	public String getLimitComponent() {
		return limitComponent;
	}

	public List<String> getTemporaryTables() {
		return temporaryTables;
	}
}
