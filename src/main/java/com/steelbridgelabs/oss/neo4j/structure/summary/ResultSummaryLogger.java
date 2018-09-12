/*
 *  Copyright 2016 SteelBridge Laboratories, LLC.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more information: http://steelbridgelabs.com
 */

package com.steelbridgelabs.oss.neo4j.structure.summary;

import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.InputPosition;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.ProfiledPlan;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Rogelio J. Baucells
 */
public class ResultSummaryLogger {

    private static class ProfileInformation {

        private static class ProfileInformationDetails {

            private int indentationLevel;
            private String operator;
            private String estimatedRows;
            private String rows;
            private String dbHits;
            private String variables;
            private String otherInformation;
        }

        private static final String OperatorColumnName = "Operator";
        private static final String EstimatedRowsColumnName = "Estimated Rows";
        private static final String RowsColumnName = "Rows";
        private static final String DBHitsColumnName = "DB Hits";
        private static final String VariablesColumnName = "Variables";
        private static final String OtherInformationColumnName = "Other";

        private static final String[] otherInformationArgumentKeys = {"LegacyExpression", "LabelName", "Index", "LegacyIndex", "KeyExpressions", "EntityByIdRhs", "ExpandExpression"};

        private final List<ProfileInformationDetails> details = new LinkedList<>();

        private int operatorLength = OperatorColumnName.length();
        private int estimatedRowsLength = EstimatedRowsColumnName.length();
        private int rowsLength = RowsColumnName.length();
        private int dbHitsLength = DBHitsColumnName.length();
        private int variablesLength = VariablesColumnName.length();
        private int otherInformationLength = OtherInformationColumnName.length();

        public void process(ProfiledPlan profilePlan) {
            Objects.requireNonNull(profilePlan, "profilePlan cannot be null");
            // process plan
            process(profilePlan, 0);
        }

        private void process(ProfiledPlan profilePlan, int indentationLevel) {
            // create details instance
            ProfileInformationDetails information = new ProfileInformationDetails();
            // operator
            information.operator = printOperator(profilePlan.operatorType(), indentationLevel);
            // arguments
            Map<String, Value> arguments = profilePlan.arguments();
            // compile information
            information.indentationLevel = indentationLevel;
            information.estimatedRows = printEstimatedRows(arguments.get("EstimatedRows"));
            information.rows = String.format(Locale.US, "%d", profilePlan.records());
            information.dbHits = String.format(Locale.US, "%d", profilePlan.dbHits());
            information.variables = profilePlan.identifiers().stream().map(String::trim).collect(Collectors.joining(", "));
            information.otherInformation = printOtherInformation(arguments);
            // append to list
            add(information);
            // children
            List<ProfiledPlan> children = profilePlan.children();
            // process children (backwards)
            for (int i = children.size() - 1; i >= 0; i--) {
                // current child
                ProfiledPlan child = children.get(i);
                // process child
                process(child, indentationLevel + i);
            }
        }

        private void add(ProfileInformationDetails information) {
            // update statistics
            operatorLength = information.operator.length() - 2 > operatorLength ? information.operator.length() - 2 : operatorLength;
            estimatedRowsLength = information.estimatedRows.length() > estimatedRowsLength ? information.estimatedRows.length() : estimatedRowsLength;
            rowsLength = information.rows.length() > rowsLength ? information.rows.length() : rowsLength;
            dbHitsLength = information.dbHits.length() > dbHitsLength ? information.dbHits.length() : dbHitsLength;
            variablesLength = information.variables.length() > variablesLength ? information.variables.length() : variablesLength;
            otherInformationLength = information.otherInformation.length() > otherInformationLength ? information.otherInformation.length() : otherInformationLength;
            // append to list
            details.add(information);
        }

        private static String printOperator(String operator, int indentationLevel) {
            // create builder
            StringBuilder builder = new StringBuilder();
            // process indentation level
            for (int i = 0; i <= indentationLevel; i++)
                builder.append("| ");
            // append operator
            builder.append("+").append(operator);
            // return text
            return builder.toString();
        }

        private static String printEstimatedRows(Value estimatedRows) {
            // format number
            return estimatedRows != null ? String.format(Locale.US, "%d", (long)estimatedRows.asDouble()) : "";
        }

        private static String printOtherInformation(Map<String, Value> arguments) {
            // format number
            return Arrays.stream(otherInformationArgumentKeys)
                .map(arguments::get)
                .filter(value -> value != null && !value.isNull())
                .map(value -> value.asObject().toString())
                .collect(Collectors.joining(", "));
        }

        @Override
        public String toString() {
            // create string builder
            StringBuilder builder = new StringBuilder("\n");
            // header
            builder.append("+-")
                .append(StringUtils.repeat("-", operatorLength)).append("-+-")
                .append(StringUtils.repeat("-", estimatedRowsLength)).append("-+-")
                .append(StringUtils.repeat("-", rowsLength)).append("-+-")
                .append(StringUtils.repeat("-", dbHitsLength)).append("-+-")
                .append(StringUtils.repeat("-", variablesLength)).append("-+-")
                .append(StringUtils.repeat("-", otherInformationLength)).append("-+\n");
            builder.append("| ")
                .append(OperatorColumnName).append(StringUtils.repeat(" ", operatorLength - OperatorColumnName.length())).append(" + ")
                .append(EstimatedRowsColumnName).append(StringUtils.repeat(" ", estimatedRowsLength - EstimatedRowsColumnName.length())).append(" + ")
                .append(RowsColumnName).append(StringUtils.repeat(" ", rowsLength - RowsColumnName.length())).append(" + ")
                .append(DBHitsColumnName).append(StringUtils.repeat(" ", dbHitsLength - DBHitsColumnName.length())).append(" + ")
                .append(VariablesColumnName).append(StringUtils.repeat(" ", variablesLength - VariablesColumnName.length())).append(" + ")
                .append(OtherInformationColumnName).append(StringUtils.repeat(" ", otherInformationLength - OtherInformationColumnName.length())).append(" |\n");
            builder.append("+-")
                .append(StringUtils.repeat("-", operatorLength)).append("-+-")
                .append(StringUtils.repeat("-", estimatedRowsLength)).append("-+-")
                .append(StringUtils.repeat("-", rowsLength)).append("-+-")
                .append(StringUtils.repeat("-", dbHitsLength)).append("-+-")
                .append(StringUtils.repeat("-", variablesLength)).append("-+-")
                .append(StringUtils.repeat("-", otherInformationLength)).append("-+\n");
            // running state
            boolean first = true;
            int lastIndentationLevel = -1;
            // loop details
            for (ProfileInformationDetails item : details) {
                // append line separator if needed
                if (!first) {
                    // check indentation level changed
                    if (lastIndentationLevel < item.indentationLevel) {
                        // process indentation level
                        for (int i = 0; i < item.indentationLevel; i++)
                            builder.append("| ");
                        // append last level
                        builder.append("|\\  ");
                    }
                    else {
                        // process indentation level
                        for (int i = 0; i <= item.indentationLevel; i++)
                            builder.append("| ");
                        // append last level
                        builder.append("| ");
                    }
                    // operator
                    builder.append(StringUtils.repeat(" ", operatorLength - item.indentationLevel * 2 - 2)).append(" +-");
                    // estimated rows
                    builder.append(StringUtils.repeat("-", estimatedRowsLength)).append("-+-");
                    // rows
                    builder.append(StringUtils.repeat("-", rowsLength)).append("-+-");
                    // db hits
                    builder.append(StringUtils.repeat("-", dbHitsLength)).append("-+-");
                    // variables
                    builder.append(StringUtils.repeat("-", variablesLength)).append("-+-");
                    // other information
                    builder.append(StringUtils.repeat("-", otherInformationLength)).append("-+");
                    // end of header
                    builder.append("\n");
                }
                // operator
                builder.append(item.operator).append(StringUtils.repeat(" ", operatorLength - item.operator.length() + 2)).append(" |");
                // estimated rows
                builder.append(" ").append(StringUtils.repeat(" ", estimatedRowsLength - item.estimatedRows.length())).append(item.estimatedRows).append(" |");
                // rows
                builder.append(" ").append(StringUtils.repeat(" ", rowsLength - item.rows.length())).append(item.rows).append(" |");
                // db hits
                builder.append(" ").append(StringUtils.repeat(" ", dbHitsLength - item.dbHits.length())).append(item.dbHits).append(" |");
                // variables
                builder.append(" ").append(item.variables).append(StringUtils.repeat(" ", variablesLength - item.variables.length())).append(" |");
                // other information
                builder.append(" ").append(item.otherInformation).append(StringUtils.repeat(" ", otherInformationLength - item.otherInformation.length())).append(" |");
                // close row
                builder.append("\n");
                // update running state
                first = false;
                lastIndentationLevel = item.indentationLevel;
            }
            // footer
            builder.append("+-")
                .append(StringUtils.repeat("-", operatorLength)).append("-+-")
                .append(StringUtils.repeat("-", estimatedRowsLength)).append("-+-")
                .append(StringUtils.repeat("-", rowsLength)).append("-+-")
                .append(StringUtils.repeat("-", dbHitsLength)).append("-+-")
                .append(StringUtils.repeat("-", variablesLength)).append("-+-")
                .append(StringUtils.repeat("-", otherInformationLength)).append("-+\n");
            // return table
            return builder.toString();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(ResultSummaryLogger.class);

    private ResultSummaryLogger() {
    }

    public static void log(ResultSummary summary) {
        Objects.requireNonNull(summary, "summary cannot be null");
        // log information
        if (logger.isInfoEnabled() && summary.hasProfile()) {
            // create builder
            StringBuilder builder = new StringBuilder();
            // append statement
            builder.append("Profile for CYPHER statement: ").append(summary.statement()).append("\n");
            // create profile information
            ProfileInformation profileInformation = new ProfileInformation();
            // process profile
            profileInformation.process(summary.profile());
            // log tabular results
            builder.append(profileInformation.toString());
            // log information
            logger.info(builder.toString());
        }
        // log notifications
        if (logger.isWarnEnabled()) {
            // loop notifications
            for (Notification notification : summary.notifications()) {
                // position if any
                InputPosition position = notification.position();
                // log information
                logger.warn("CYPHER statement [{}] notification; severity: {}, code: {}, title: {}, description: {}{}", summary.statement(), notification.severity(), notification.code(), notification.title(), notification.description(), position != null ? ", [line: " + position.line() + ", position: " + position.column() + ", offset: " + position.offset() + "]" : "");
            }
        }
    }
}
