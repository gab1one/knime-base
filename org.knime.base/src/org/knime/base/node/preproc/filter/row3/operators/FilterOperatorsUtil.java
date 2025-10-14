/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Sep 23, 2025 (Paul Bärnreuther): created
 */
package org.knime.base.node.preproc.filter.row3.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.knime.base.node.preproc.filter.row3.operators.defaults.DefaultComparableOperators;
import org.knime.base.node.preproc.filter.row3.operators.defaults.DefaultEqualityOperators;
import org.knime.base.node.preproc.filter.row3.operators.defaults.FallbackOperatorParameters;
import org.knime.base.node.preproc.filter.row3.operators.legacy.LegacyFilterParameters;
import org.knime.base.node.preproc.filter.row3.operators.missing.IsMissingFilterOperator;
import org.knime.base.node.preproc.filter.row3.operators.missing.IsNotMissingFilterOperator;
import org.knime.base.node.preproc.filter.row3.operators.pattern.PatternFilterUtils;
import org.knime.base.node.preproc.filter.row3.operators.pattern.RegexPatternFilterOperator;
import org.knime.base.node.preproc.filter.row3.operators.pattern.RowKeyRegexPatternFilterOperator;
import org.knime.base.node.preproc.filter.row3.operators.pattern.RowKeyWildcardPatternFilterOperator;
import org.knime.base.node.preproc.filter.row3.operators.pattern.RowNumberRegexPatternFilterOperator;
import org.knime.base.node.preproc.filter.row3.operators.pattern.RowNumberWildcardPatternFilterOperator;
import org.knime.base.node.preproc.filter.row3.operators.pattern.WildcardPatternFilterOperator;
import org.knime.core.data.BoundedValue;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.extensions.filtervalue.FilterOperator;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.extensions.filtervalue.FilterOperatorsRegistry;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.extensions.filtervalue.FilterValueParameters;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.extensions.filtervalue.TypeMappingUtils;

/**
 * Utility class that extends the FilterOperatorsRegistry functionality by adding predicate-based operators (like
 * pattern matching and missing value checks) that can work with multiple data types.
 *
 * @author Paul Bärnreuther
 */
public final class FilterOperatorsUtil {

    private FilterOperatorsUtil() {
        // Utility class
    }

    /**
     * The predicate instance that always returns false.
     */
    public static final Predicate<DataValue> PREDICATE_ALWAYS_FALSE = dataValue -> false;

    /**
     * The predicate instance that always returns true.
     */
    public static final Predicate<DataValue> PREDICATE_ALWAYS_TRUE = dataValue -> true;

    /**
     * Interface for grouping operators with their applicability logic.
     */
    private interface OperatorGroup {
        List<FilterOperator<? extends FilterValueParameters>> getOperators(final DataType dataType);

        default boolean isApplicable(final DataType dataType) {
            return true;
        }
    }

    /**
     * Default operator groups available for column filtering.
     */
    private static final List<OperatorGroup> DEFAULT_COLUMN_OPERATOR_GROUPS = List.of( //
        // Default equality (fallback)
        new OperatorGroup() {
            @Override
            public List<FilterOperator<? extends FilterValueParameters>> getOperators(final DataType dataType) {
                return DefaultEqualityOperators.getOperators(dataType);
            }

            @Override
            public boolean isApplicable(final DataType dataType) {
                return TypeMappingUtils.supportsDataType(dataType);
            }
        },
        // Default comparable (fallback)
        new OperatorGroup() {
            @Override
            public List<FilterOperator<? extends FilterValueParameters>> getOperators(final DataType dataType) {
                return DefaultComparableOperators.getOperators(dataType);
            }

            @Override
            public boolean isApplicable(final DataType dataType) {
                return TypeMappingUtils.supportsDataType(dataType) && dataType.isCompatible(BoundedValue.class);
            }
        },
        //
        // Pattern matching
        new OperatorGroup() {
            @Override
            public List<FilterOperator<? extends FilterValueParameters>> getOperators(final DataType dataType) {
                return List.of(new RegexPatternFilterOperator(), new WildcardPatternFilterOperator());
            }

            @Override
            public boolean isApplicable(final DataType dataType) {
                return PatternFilterUtils.isSupported(dataType);
            }
        }, //
        // Missing
        dataType -> List.of(IsMissingFilterOperator.INSTANCE, IsNotMissingFilterOperator.INSTANCE)//
    );

    /**
     * Default operators available for row key filtering.
     */
    private static final List<RowKeyFilterOperator<? extends FilterValueParameters>> DEFAULT_ROW_KEY_OPERATORS =
        List.of(//
            new RowKeyRegexPatternFilterOperator(), //
            new RowKeyWildcardPatternFilterOperator() //
        );

    /**
     * Default operators available for row number filtering.
     */
    private static final List<RowNumberFilterOperator<? extends FilterValueParameters>> DEFAULT_ROW_NUMBER_OPERATORS =
        List.of(//
            new RowNumberRegexPatternFilterOperator(), //
            new RowNumberWildcardPatternFilterOperator() //
        );

    /**
     * Gets all filter operators for the given data type, including both exact-match operators from the registry and
     * predicate-based operators that support the data type. Deduplicates operators by ID, preferring registry operators
     * over pattern operators.
     *
     * @param dataType the data type to get operators for
     * @return list of deduplicated filter operators for UI display
     */
    public static List<FilterOperator<FilterValueParameters>> getOperators(final DataType dataType) {
        return deduplicateOperatorsById(getAllColumnOperators(dataType));
    }

    /**
     * Gets all filter operators for the given data type, including both exact-match operators from the registry and
     * predicate-based operators that support the data type. Does NOT deduplicate - returns all operators including
     * those with duplicate IDs.
     *
     * @param dataType the data type to get operators for
     * @return list of all applicable filter operators including duplicates
     */
    public static List<FilterOperator<FilterValueParameters>> getAllColumnOperators(final DataType dataType) {
        final var registryOperators = FilterOperatorsRegistry.getInstance().getOperators(dataType);
        final var defaultOperators = DEFAULT_COLUMN_OPERATOR_GROUPS.stream()
            .filter(group -> group.isApplicable(dataType)).flatMap(group -> group.getOperators(dataType).stream());

        return Stream.concat(defaultOperators, registryOperators.stream())
            .map(c -> (FilterOperator<FilterValueParameters>)c).toList();
    }

    /**
     * Finds the matching operator for the given operator ID and parameter class. This method handles backwards
     * compatibility by checking both operator ID and parameter class. Multiple operators may have the same ID (e.g.,
     * registry operators and pattern operators both providing "REGEX"), so we use the parameter class to disambiguate
     * and find the exact operator that was used when the workflow was saved.
     */
    public static Optional<FilterOperator<FilterValueParameters>> findMatchingColumnOperator(final DataType columnType,
        final String operatorId, final FilterValueParameters parameters) {
        final var allAvailableOperators = getAllColumnOperators(columnType);
        final var parameterClass = parameters == null ? null : parameters.getClass();
        return allAvailableOperators.stream() //
            .filter(op -> op.getId().equals(operatorId)) //
            .filter(op -> Objects.equals(op.getNodeParametersClass(), parameterClass)) //
            .findFirst();

    }

    /**
     * Gets all currently registered parameter classes, including those from predicate-based operators.
     *
     * @return all currently registered parameter classes
     */
    public static List<Class<? extends FilterValueParameters>> getAllParameterClasses() {
        final var registryClasses = FilterOperatorsRegistry.getInstance().getAllParameterClasses();
        final var defaultClasses = Stream.of(
            DEFAULT_COLUMN_OPERATOR_GROUPS.stream().flatMap(group -> group.getOperators(null).stream())
                .map(op -> op.getNodeParametersClass()),
            DEFAULT_ROW_KEY_OPERATORS.stream().map(op -> op.getNodeParametersClass()),
            DEFAULT_ROW_NUMBER_OPERATORS.stream().map(op -> op.getNodeParametersClass()),
            Stream.of(LegacyFilterParameters.class), //
            Stream.of(FallbackOperatorParameters.class) // Add default operator parameters
        ).flatMap(Function.identity());

        return Stream.concat(registryClasses.stream(), defaultClasses).distinct().filter(Objects::nonNull).toList();
    }

    public static List<RowNumberFilterOperator<? extends FilterValueParameters>> getRowNumberOperators() {
        return DEFAULT_ROW_NUMBER_OPERATORS;
    }

    public static List<RowKeyFilterOperator<? extends FilterValueParameters>> getRowKeyOperators() {
        return DEFAULT_ROW_KEY_OPERATORS;
    }

    public static Optional<RowNumberFilterOperator<? extends FilterValueParameters>>
        findMatchingRowNumberOperator(final String operatorId) {
        return DEFAULT_ROW_NUMBER_OPERATORS.stream() //
            .filter(op -> op.getId().equals(operatorId)) //
            .findFirst();
    }

    public static Optional<RowKeyFilterOperator<? extends FilterValueParameters>>
        findMatchingRowKeyOperator(final String operatorId) {
        return DEFAULT_ROW_KEY_OPERATORS.stream() //
            .filter(op -> op.getId().equals(operatorId)) //
            .findFirst();
    }

    /**
     * Deduplicates operators by ID. The incoming operators first list the internal default operators and then the ones
     * from the registry. We keep the order but remove duplicates by ID by using the latter but keeping the position of
     * the former if two IDs are the same. E.g. for input [A (1), B (2), A (3), C (4)] we return [A (3), B (2), C (4)].
     *
     * @param operators list of operators potentially containing duplicates
     * @return deduplicated list of operators where the last occurrence of each ID is kept at the position of the first
     *         occurrence
     */
    private static List<FilterOperator<FilterValueParameters>>
        deduplicateOperatorsById(final List<FilterOperator<FilterValueParameters>> operators) {
        final Map<String, Integer> idToIndex = new HashMap<>();
        final List<FilterOperator<FilterValueParameters>> uniqueOperators = new ArrayList<>();
        for (int i = 0; i < operators.size(); i++) {
            final var operator = operators.get(i);
            if (idToIndex.containsKey(operator.getId())) {
                // replace existing operator with the new one, keep position
                final var existingIndex = idToIndex.get(operator.getId());
                uniqueOperators.set(existingIndex, operator);
            } else {
                // new operator, add at the end
                final var nextIndex = uniqueOperators.size();
                uniqueOperators.add(operator);
                idToIndex.put(operator.getId(), nextIndex);
            }
        }
        return uniqueOperators;

    }

}