/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.sql.parsers.rewriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.SqlCompilationException;


public class AliasApplier implements QueryRewriter {

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    Map<String, Expression> aliasMap = extractAlias(pinotQuery.getSelectList());
    applyAlias(aliasMap, pinotQuery);
    return pinotQuery;
  }

  private static Map<String, Expression> extractAlias(List<Expression> selectExpressions) {
    Map<String, Expression> aliasMap = new HashMap<>();
    for (Expression expression : selectExpressions) {
      Function function = expression.getFunctionCall();
      if (function == null) {
        continue;
      }
      // For DISTINCT queries the aliased columns are nested as arguments of the distinct function
      // (e.g. distinct(as(col, alias), ...)), so unwrap it to pick up those aliases as well. This lets the
      // aliases be referenced in ORDER BY, consistent with non-DISTINCT queries.
      if (function.getOperator().equals("distinct")) {
        for (Expression distinctOperand : function.getOperands()) {
          extractAlias(distinctOperand, aliasMap);
        }
        continue;
      }
      extractAlias(expression, aliasMap);
    }
    return aliasMap;
  }

  private static void extractAlias(Expression expression, Map<String, Expression> aliasMap) {
    Function function = expression.getFunctionCall();
    if (function == null || !function.getOperator().equals("as")) {
      return;
    }
    String alias = function.getOperands().get(1).getIdentifier().getName();
    if (aliasMap.put(alias, function.getOperands().get(0)) != null) {
      throw new SqlCompilationException("Find duplicate alias: " + alias);
    }
  }

  private static void applyAlias(Map<String, Expression> aliasMap, PinotQuery pinotQuery) {
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null) {
      for (Expression expression : groupByList) {
        applyAlias(aliasMap, expression);
      }
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      applyAlias(aliasMap, havingExpression);
    }
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (orderByList != null) {
      for (Expression expression : orderByList) {
        applyAlias(aliasMap, expression);
      }
    }
  }

  private static void applyAlias(Map<String, Expression> aliasMap, Expression expression) {
    Identifier identifier = expression.getIdentifier();
    if (identifier != null) {
      Expression aliasExpression = aliasMap.get(identifier.getName());
      if (aliasExpression != null) {
        expression.setType(aliasExpression.getType());
        expression.setIdentifier(aliasExpression.getIdentifier());
        expression.setFunctionCall(aliasExpression.getFunctionCall());
        expression.setLiteral(aliasExpression.getLiteral());
      }
      return;
    }
    Function function = expression.getFunctionCall();
    if (function != null) {
      for (Expression operand : function.getOperands()) {
        applyAlias(aliasMap, operand);
      }
    }
  }
}
