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
 *   20 Mar 2024 (jasper): created
 */
package org.knime.base.node.preproc.filter.row3;

import static org.knime.base.node.preproc.filter.row3.RowIdentifiers.ROW_ID;
import static org.knime.base.node.preproc.filter.row3.RowIdentifiers.ROW_NUMBER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.knime.base.data.filter.row.v2.IndexedRowReadPredicate;
import org.knime.base.node.preproc.filter.row3.AbstractRowFilterNodeSettings.FilterCriterion.SelectedColumnRef;
import org.knime.base.node.preproc.filter.row3.FilterOperator.InternalFilterOperator;
import org.knime.base.node.preproc.filter.row3.predicates.PredicateFactories;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.data.DataValueComparatorDelegator;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.v2.RowRead;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.util.Pair;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.ClassIdStrategy;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.DefaultClassIdStrategy;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.DynamicParameters;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.DynamicParameters.DynamicParametersProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.DynamicValuesInput;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.extensions.filtervalue.BuiltinOperator;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.extensions.filtervalue.BuiltinOperator.SingleCellOperatorFamily;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.extensions.filtervalue.FilterValueParameters;
import org.knime.core.webui.node.dialog.defaultdialog.setting.columnselection.ColumnSelectionToStringOrEnumMigration;
import org.knime.core.webui.node.dialog.defaultdialog.setting.singleselection.StringOrEnum;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.handler.WidgetHandlerException;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.array.ArrayWidget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.HorizontalLayout;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.Migration;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.EnumChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.StringChoice;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.util.AllColumnsProvider;

/**
 * Shared settings class for the Filter and Splitter nodes.
 *
 * @author Manuel Hotz, KNIME GmbH, Konstanz, Germany
 * @author Jasper Krauter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // webui
abstract class AbstractRowFilterNodeSettings implements NodeParameters {

    enum Criteria {
            @Label("All criteria")
            AND, //
            @Label("Any criterion")
            OR;

        boolean isAnd() {
            return this == AND;
        }
    }

    @Widget(title = "Match row if matched by", description = """
            Match the row if all or any criteria match:
            <ul>
                <li><b>All criteria</b>: a row is matched if <i>all</i> of the criteria match
                (intersection of matches)</li>
                <li><b>Any criterion</b>: a row is matched if <i>at least one</i> of the
                criteria matches (union of matches)</li>
            </ul>
            """)
    @Effect(predicate = HasMultipleFilterConditions.class, type = EffectType.SHOW)
    @ValueSwitchWidget
    @Layout(DialogSections.Filter.AllAny.class)
    Criteria m_matchCriteria = Criteria.AND;

    static class FilterCriterion implements NodeParameters {

        interface Condition {
            @HorizontalLayout
            interface ColumnOperator {
                interface Column {
                }

                interface Operator {
                }
            }

            interface Modifier {
            }

            interface ValueInput {
            }
        }

        @Widget(title = "Filter column", description = """
                The column on which to apply the filter.
                <br />

                The special column "RowID" represents the RowID of the input and is treated as a String column that
                is never missing. The special column "Row number" targets the 1-based row number of the input, is
                treated as a Long column and is never missing. Both special columns are always available, regardless of
                the input table spec or data.
                <br /><br />

                Columns containing data types that are non-native, i.e. contain cells of heterogeneous data types, or
                do not offer a conversion from and to a string representation are not supported and are filtered out
                from the available options.
                <br />

                Collection columns are also not supported by the node.
                """)
        @ChoicesProvider(AllColumnsProvider.class)
        @Layout(Condition.ColumnOperator.Column.class)
        @ValueReference(SelectedColumnRef.class)
        @Migration(FromColumnSelectionMigration.class)
        @Persist(configKey = "columnV2")
        StringOrEnum<RowIdentifiers> m_column = new StringOrEnum<>(ROW_ID);

        static final class FromColumnSelectionMigration extends ColumnSelectionToStringOrEnumMigration<RowIdentifiers> {

            FromColumnSelectionMigration() {
                super("column");
            }

            @Override
            public Optional<RowIdentifiers> loadEnumFromLegacyString(final String legacyString) {
                if (LEGACY_ROW_KEYS_IDENTIFIER.equals(legacyString)) {
                    return Optional.of(ROW_ID);
                }
                if (LEGACY_ROW_NUMBERS_IDENTIFIER.equals(legacyString)) {
                    return Optional.of(ROW_NUMBER);
                }
                return Optional.empty();
            }
        }

        static final class SelectedColumnRef implements ParameterReference<StringOrEnum<RowIdentifiers>> {
        }

        /**
         * The type of the currently selected column when in a dialog, otherwise the type of the selected column the
         * last time the dialog was applied. null if no column is selected (i.e. ROW_ID or ROW_NUMBER is selected) or
         * the selected column is missing and the dialog is opened.
         */
        @ValueProvider(DataTypeProvider.class)
        DataType m_columnType;

        static final class DataTypeProvider implements StateProvider<DataType> {

            private Supplier<StringOrEnum<RowIdentifiers>> m_selectedColumn;

            @Override
            public void init(final StateProviderInitializer initializer) {
                initializer.computeBeforeOpenDialog();
                m_selectedColumn = initializer.computeFromValueSupplier(SelectedColumnRef.class);
            }

            @Override
            public DataType computeState(final NodeParametersInput context) throws StateComputationFailureException {
                final var selectedColumn = m_selectedColumn.get();
                if (selectedColumn.getEnumChoice().isPresent()) {
                    return null;
                }
                return context.getInTableSpec(0)
                    .flatMap(s -> Optional.ofNullable(s.getColumnSpec(selectedColumn.getStringChoice())))
                    .map(DataColumnSpec::getType).orElse(null);
            }
        }

        static final class OperatorsProvider implements StringChoicesProvider {

            private Supplier<DataType> m_dataType;

            @Override
            public void init(final StateProviderInitializer initializer) {
                initializer.computeBeforeOpenDialog();
                m_dataType = initializer.computeFromProvidedState(DataTypeProvider.class);
            }

            @Override
            public List<StringChoice> computeState(final NodeParametersInput context) {
                final var dataType = m_dataType.get();
                if (dataType == null) {
                    return List.of();
                }
                final var availableOperators = FilterOperator2Util.getOperatorsForColumn(dataType);
                return FilterOperator2Util.getSortedChoices(availableOperators);
            }

        }

        // We explicitly do not "reset" the current operator to one applicable for the current column data type,
        // in order to allow the user to switch between columns without resetting their operator selection.
        @Widget(title = "Operator", description = """
                The operator determines whether a value meets the filter criterion. A value matches the criterion if the
                applied operator returns "true" and does not match if it returns "false."
                Not all operators are compatible with every column data type. Only the applicable operators are shown
                for the selected column.
                <br /><br />

                <b>Missing value handling:</b> The "is missing" and "is not missing" operators are available for all
                data types. Most other operators do not evaluate on missing values and will not match if a missing cell
                is encountered. An exception is the "is not equal" operator, which matches missing cells because they
                cannot equal the reference value. To exclude missing cells in this case, use the "is not equal (nor
                missing)" operator. "RowID" and "Row number" special columns are never missing.
                """)
        @Layout(Condition.ColumnOperator.Operator.class)
        @ValueReference(OperatorRef.class)
        @ChoicesProvider(TypeBasedOperatorsProvider.class)
        FilterOperator m_operator = FilterOperator.EQ;

        static class OperatorRef implements ParameterReference<FilterOperator> {
        }

        @Widget(title = "Operator (extension point)", description = "")
        @Layout(Condition.ColumnOperator.Operator.class)
        @ValueReference(OperatorIdRef.class)
        @ChoicesProvider(OperatorsProvider.class)
        String m_operatorId = "EQ";

        static class OperatorIdRef implements ParameterReference<String> {
        }

        FilterCriterion() {
            // serialization
            this((DataColumnSpec)null);
        }

        FilterCriterion(final NodeParametersInput ctx) {
            // set last supported column as default column, like old Row Filter did
            this(ctx.getInTableSpec(0).stream().flatMap(DataTableSpec::stream).reduce((f, s) -> s).orElse(null));
        }

        FilterCriterion(final DataColumnSpec colSpec) {
            if (colSpec == null) {
                // we don't know how RowIDs look in general, since they can be user-defined, hence we just put
                // a placeholder here that is not null
                m_predicateValues = DynamicValuesInput.forRowID();
                return;
            }
            m_column = new StringOrEnum<>(colSpec.getName());
            if (DynamicValuesInput.supportsDataType(colSpec.getType())) {
                m_predicateValues =
                    DynamicValuesInput.singleValueWithCaseMatchingForStringWithDefault(colSpec.getType());
            } else {
                m_predicateValues = DynamicValuesInput.emptySingle();
            }
        }

        void validate(final DataTableSpec spec) throws InvalidSettingsException {
            // check table slicing (filter on numeric row number values)
            if (isFilterOnRowNumbers() && RowNumberFilterSpec.supportsOperator(m_operator)) {
                RowNumberFilterSpec.toFilterSpec(this);
                return;
            }

            // validate using filter on row read (i.e. values)
            toPredicate(spec);
        }

        IndexedRowReadPredicate toPredicate(final DataTableSpec spec) throws InvalidSettingsException {

            final var rowIdentifierChoice = m_column.getEnumChoice();
            if (rowIdentifierChoice.isPresent()) {
                return switch (rowIdentifierChoice.get()) {
                    case ROW_ID -> rowKeyPredicate(m_predicateValues);
                    case ROW_NUMBER -> rowNumberPredicate(m_predicateValues);
                };
            }

            final var column = m_column.getStringChoice();
            final var columnIndex = spec.findColumnIndex(column);
            if (columnIndex < 0) {
                throw new InvalidSettingsException("Column \"%s\" could not be found in input table".formatted(column));
            }
            final var extensionOperators = FilterOperator2Util.EXTENSION_FILTER_OPERATORS.get(m_columnType);

            final var availableOperators = FilterOperator2Util.getOperatorsForColumn(m_columnType);

            if (extensionOperators != null) {
                final var selectedOperator = extensionOperators.stream().filter(op -> {
                    if (op instanceof BuiltinOperator bop) {
                        return FilterOperator2Util.idsForBuiltinOperator(bop).toList().contains(m_operatorId);
                    } else {
                        return m_operatorId.equals(op.getId());
                    }
                }).findFirst();
                if (selectedOperator.isPresent()
                    && selectedOperator.get().getNodeParametersClass().isInstance(m_filterValueParameters)) {
                    final var operator = selectedOperator.get();
                    if (operator instanceof SingleCellOperatorFamily) {
                        // then we can be sure that the parameters are SingleCellValueParameters
                        final var singleValueParameters =
                            (FilterValueParameters.SingleCellValueParameters<?>)m_filterValueParameters;
                        final var parametersType = singleValueParameters.getSpecificType();
                        final var columnType = spec.getColumnSpec(columnIndex).getType();
                        final DataType comparisonType;
                        if (parametersType.isASuperTypeOf(columnType)) {
                            comparisonType = parametersType;
                        } else if (columnType.isASuperTypeOf(parametersType)) {
                            comparisonType = columnType;
                        } else {
                            throw new InvalidSettingsException( // TODO message
                                "The filter value type (%s) is not compatible with the column type (%s)"
                                    .formatted(parametersType, columnType));
                        }
                        final var comparator =
                            new DataValueComparatorDelegator<DataValue>(comparisonType.getComparator());
                        final var comparisonValue = singleValueParameters.createCell();
                        final Function<DataValue, Integer> compare = v -> comparator.compare(v, comparisonValue);
                        if ("NEQ".equals(m_operatorId)) {
                            return new IndexedRowReadPredicate() {

                                @Override
                                public boolean test(final long index, final RowRead read) {
                                    if (read.isMissing(columnIndex)) {
                                        return true;
                                    }
                                    return compare.apply(read.getValue(columnIndex)) != 0;
                                }
                            };
                        } else {
                            Predicate<Integer> comparisonResultPredicate = switch (m_operatorId) {
                                case "EQ" -> r -> r == 0;
                                case "NEQ_MISS" -> r -> r != 0;
                                case "LT" -> r -> r < 0;
                                case "LTE" -> r -> r <= 0;
                                case "GT" -> r -> r > 0;
                                case "GTE" -> r -> r >= 0;
                                default -> throw new IllegalStateException("Unhandled operator id: " + m_operatorId);
                            };
                            return new IndexedRowReadPredicate() {

                                @Override
                                public boolean test(final long index, final RowRead read) {
                                    if (read.isMissing(columnIndex)) {
                                        return false;
                                    }
                                    return comparisonResultPredicate.test(compare.apply(read.getValue(columnIndex)));
                                }
                            };

                        }
                    }

                    final var predicate = operator.createPredicateUnsafe(m_filterValueParameters);
                    return new IndexedRowReadPredicate() {

                        @Override
                        public boolean test(final long index, final RowRead read) {
                            if (read.isMissing(columnIndex)) {
                                return false;
                            }
                            try {
                                return predicate.test(read.getValue(columnIndex));
                            } catch (InvalidSettingsException e) {
                                // TODO: Maybe runtime exception instead of ISE?
                                throw new RuntimeException(
                                    "Could not evaluate predicate for column \"%s\" with value %s".formatted(column,
                                        read.getValue(columnIndex)),
                                    e);
                            }
                        }

                    };
                }
            }

            // TODO: For future backwards-compatibility we should check internal operators before extension operators
            final var internalFilterOperator = availableOperators.internalBuiltinOperators().stream()
                .filter(ifo -> ifo.getFirst().name().equals(m_operatorId)).findFirst().map(Pair::getSecond);

            if (internalFilterOperator.isPresent()) {
                return InternalFilterOperator.toPredicate(internalFilterOperator.get(), m_filterValueParameters,
                    OptionalInt.of(columnIndex), m_columnType);
            }

            return m_operator.translateToPredicate(m_predicateValues, columnIndex,
                spec.getColumnSpec(columnIndex).getType());
        }

        private IndexedRowReadPredicate rowKeyPredicate(final DynamicValuesInput predicateValues)
            throws InvalidSettingsException {
            return PredicateFactories //
                .getRowKeyPredicateFactory(m_operator) //
                .orElseThrow(() -> new InvalidSettingsException( //
                    "Unsupported operator \"%s\" for RowID comparison".formatted(m_operator.name()))) //
                .createPredicate(OptionalInt.empty(), predicateValues);
        }

        private IndexedRowReadPredicate rowNumberPredicate(final DynamicValuesInput predicateValues)
            throws InvalidSettingsException {
            return PredicateFactories //
                .getRowNumberPredicateFactory(m_operator) //
                .orElseThrow(() -> new InvalidSettingsException( //
                    "Unsupported operator \"%s\" for row number comparison".formatted(m_operator.name()))) //
                .createPredicate(OptionalInt.empty(), predicateValues);
        }

        boolean isFilterOnRowNumbers() {
            return m_column.getEnumChoice().map(ROW_NUMBER::equals).orElse(false);
        }

        @Widget(title = "Filter value", description = """
                The value for the filter criterion.
                <br/><br />

                <i>Note:</i> Currently, comparison values for non-numeric and non-string data types, e.g.
                date&amp;time-based types, must be entered as its string representation like in the <a href="
                """ + ExternalLinks.HUB_TABLE_CREATOR + """
                "><i>Table Creator</i></a> node.
                <br/>

                The format for date&amp;time-based values is "ISO-8601 extended". For example, a "Local Date" must be
                entered in the format "2006-07-28". More information can be obtained from the ISO patterns in the
                "Predefined Formatters" table of the <a href="
                """ + ExternalLinks.ISO_DATETIME_PATTERNS + """
                ">Java SE 17 documentation</a>.
                        """)
        @Layout(Condition.ValueInput.class)
        @ValueProvider(TypeAndOperatorBasedInput.class)
        @ValueReference(DynamicValuesInputRef.class)
        DynamicValuesInput m_predicateValues = DynamicValuesInput.emptySingle();

        @Layout(Condition.ValueInput.class)
        @DynamicParameters(FilterValueParametersProvider.class)
        @ValueReference(CurrentFilterValueParametersRef.class)
        FilterValueParameters m_filterValueParameters;

        static final class CurrentFilterValueParametersRef implements ParameterReference<FilterValueParameters> {
        }

        static class FilterValueParametersProvider implements DynamicParametersProvider<FilterValueParameters> {

            private Supplier<DataType> m_dataType;

            private Supplier<String> m_currentOperatorId;

            private Supplier<FilterValueParameters> m_currentValue;

            @Override
            public void init(final StateProviderInitializer initializer) {
                initializer.computeBeforeOpenDialog();
                m_dataType = initializer.computeFromProvidedState(DataTypeProvider.class);
                m_currentOperatorId = initializer.computeFromValueSupplier(OperatorIdRef.class);
                m_currentValue = initializer.getValueSupplier(CurrentFilterValueParametersRef.class);
            }

            @Override
            public ClassIdStrategy<FilterValueParameters> getClassIdStrategy() {
                final Collection<Class<? extends FilterValueParameters>> possibleClasses = new ArrayList<>();
                for (var operators : FilterOperator2Util.EXTENSION_FILTER_OPERATORS.values()) {
                    for (var operator : operators) {
                        possibleClasses.add(operator.getNodeParametersClass());
                    }
                }
                possibleClasses.addAll(FilterOperator.InternalFilterOperator.ALL_PARAMETER_CLASSES);
                return new DefaultClassIdStrategy<>(possibleClasses);
            }

            @Override
            public FilterValueParameters computeParameters(final NodeParametersInput parametersInput)
                throws StateComputationFailureException {
                final var dataType = m_dataType.get();
                if (dataType == null) {
                    throw new StateComputationFailureException("No column selected");
                }
                final var availableOperators = FilterOperator2Util.getOperatorsForColumn(dataType);
                final var currentOperatorId = m_currentOperatorId.get();
                Class<? extends FilterValueParameters> targetClass;
                try {
                    targetClass = FilterOperator2Util.getTargetClass(availableOperators, currentOperatorId);
                } catch (InvalidSettingsException e) {
                    throw new StateComputationFailureException("Not implemented yet ");
                }
                final var currentValue = m_currentValue.get();
                if (currentValue != null && targetClass.equals(currentValue.getClass())) {
                    return currentValue;
                }
                try {
                    final var constructor = targetClass.getDeclaredConstructor();
                    constructor.setAccessible(true);
                    final var newInstance = constructor.newInstance();
                    if (currentValue != null) {
                        newInstance.applyStash(currentValue.stash(null), null);
                    }
                    return newInstance;
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "Could not instantiate FilterValueParameters of type " + targetClass, e);
                }

            }

        }

        static class DynamicValuesInputRef implements ParameterReference<DynamicValuesInput> {

        }

        static class TypeAndOperatorBasedInput implements StateProvider<DynamicValuesInput> {

            private Supplier<StringOrEnum<RowIdentifiers>> m_selectedColumn;

            private Supplier<DynamicValuesInput> m_currentValue;

            private Supplier<FilterOperator> m_currentOperator;

            @Override
            public void init(final StateProviderInitializer initializer) {
                m_selectedColumn = initializer.computeFromValueSupplier(SelectedColumnRef.class);
                m_currentOperator = initializer.computeFromValueSupplier(OperatorRef.class);
                /**
                 * Necessary, since the TypeBasedOperatorChoice does not have OperatorRef as trigger, only as a
                 * dependency
                 */
                initializer.computeOnValueChange(OperatorRef.class);
                m_currentValue = initializer.getValueSupplier(DynamicValuesInputRef.class);
            }

            @Override
            public DynamicValuesInput computeState(final NodeParametersInput context) {
                final var inputSpec = context.getInTableSpec(0);
                // spec empty, e.g. when
                // - nothing connected
                // - consuming a component input that does not have executed predecessors
                if (inputSpec.isEmpty()) {
                    // show any existing value
                    return m_currentValue.get();
                }
                if (!m_currentOperator.get().isBinary()) {
                    // we don't need an input field
                    return DynamicValuesInput.emptySingle();
                }

                final var selectedColumn = m_selectedColumn.get();
                final var dts = inputSpec.get();

                final var rowIdentifierChoice = selectedColumn.getEnumChoice();
                if (rowIdentifierChoice.isPresent()) {
                    final var newDynamicValue = switch (rowIdentifierChoice.get()) {
                        case ROW_ID -> DynamicValuesInput.forRowID();
                        case ROW_NUMBER -> DynamicValuesInput.forRowNumber(LongCell.TYPE);
                    };
                    return keepCurrentValueIfPossible(newDynamicValue);
                }

                final var columnSpec = dts.getColumnSpec(selectedColumn.getStringChoice());
                final var operatorRequiredType = m_currentOperator.get().getRequiredInputType();
                if (columnSpec == null) {
                    // we don't know the column type, but we still have the (user-supplied) comparison value,
                    // which we don't want to clear
                    return m_currentValue.get();
                }
                // provide an input field for the given type, if we can typemap it, or fall back to the column type
                // if the operator does not require a specific type
                final var columnType = columnSpec.getType();
                final var type = operatorRequiredType.orElse(columnType);
                if (DynamicValuesInput.supportsDataType(type)) {
                    final var defaultValue = DynamicValuesInput.singleValueWithCaseMatchingForStringWithDefault(type);
                    return keepCurrentValueIfPossible(defaultValue);
                }
                // cannot provide an input field
                return DynamicValuesInput.emptySingle();
            }

            private DynamicValuesInput keepCurrentValueIfPossible(final DynamicValuesInput newValue) {
                final var currentValue = m_currentValue.get();
                // types match exactly
                if (currentValue.isConfigurableFrom(newValue)) {
                    return currentValue;
                }
                // try to convert via String
                return currentValue.convertToType(newValue).orElse(newValue);
            }
        }
    }

    interface PredicatesRef extends ParameterReference<FilterCriterion[]> {
    }

    static final class HasMultipleFilterConditions implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getArray(PredicatesRef.class).hasMultipleItems();
        }
    }

    @Widget(title = "Filter criteria", description = "The list of criteria that should be filtered on.")
    @ArrayWidget(elementTitle = "Criterion", showSortButtons = true, addButtonText = "Add filter criterion",
        elementDefaultValueProvider = DefaultFilterCriterionProvider.class)
    @Layout(DialogSections.Filter.Conditions.class)
    @ValueReference(PredicatesRef.class)
    FilterCriterion[] m_predicates;

    @Widget(title = "Column domains", description = """
            Specify whether to take domains of all input columns as output domains as-is or compute them on the output
            rows.
            <br />

            Depending on the use case, one or the other setting may be preferable:
            <ul>
                <li><em>Retaining</em> input columns can be useful, if the axis limits of a view should be derived from
                domain bounds, and that bounds should stay stable even when the displayed data is filtered.
                </li>
                <li><em>Computing</em> domains can be useful when a selection widget consumes the output and should only
                display actually present options to users.</li>
            </ul>

            If column domains are irrelevant for a particular use case, the &quot;Retain&quot; option should be used
            since it does not incur computation costs.
            <br />

            For more control over individual column domains, you can use the <a href="
            """ + ExternalLinks.HUB_DOMAIN_CALCULATOR + """
                    "><em>Domain Calculator</em></a>, <a href="
            """ + ExternalLinks.HUB_EDIT_NUMERIC_DOMAIN + """
                    "><em>Edit Numeric Domain</em></a>, or <a href="
            """ + ExternalLinks.HUB_EDIT_NOMINAL_DOMAIN + """
                    "><em>Edit Nominal Domain</em></a> nodes.
            """)
    @ValueSwitchWidget()
    @Layout(DialogSections.Output.Domain.class)
    ColumnDomains m_domains = ColumnDomains.RETAIN;

    enum ColumnDomains {
            @Label(value = "Retain",
                description = """
                        Retain input domains on output columns, i.e. the upper and lower bounds or possible values in the table
                        spec are not changed, even if one of the bounds or one value is fully filtered out from the output
                        table. If the input does not contain domain information, so will the output.
                            """)
            RETAIN, @Label(value = "Compute",
                description = """
                        Compute column domains on output columns, i.e. upper and lower bounds and possible values are computed
                        only on the rows output by the node.
                            """)
            COMPUTE;
    }

    /**
     * Mode to determine which set of rows is output at the first output port (and second in case of a splitter).
     */
    enum FilterMode {
            /**
             * Include matching rows at the first port.
             */
            @Label("Output matching rows")
            MATCHING, //
            /**
             * Exclude matching rows from the first port.
             */
            @Label("Output non-matching rows")
            NON_MATCHING
    }

    /**
     * Get the output mode, i.e. output only matching rows or only non-matching rows in the first output. The second
     * output, if present, will receive the complementary set of rows.
     *
     * @return {@link FilterMode#MATCHING} if only matching rows should be output (in the first output) or
     *         {@link FilterMode#NON_MATCHING} for only non-matching.
     */
    abstract FilterMode outputMode();

    boolean outputMatches() {
        return outputMode() == FilterMode.MATCHING;
    }

    // constructor needed for de-/serialisation

    AbstractRowFilterNodeSettings() {
        this(null);
    }

    // auto-configuration
    AbstractRowFilterNodeSettings(@SuppressWarnings("unused") final NodeParametersInput ctx) { // NOSONAR
        // we don't add a filter criterion automatically in order to avoid setting a default value without
        // the user noticing (and we need to set some default value in the filter criterion, s.t. flow variables work
        // correctly)
        m_predicates = new FilterCriterion[0];
    }

    void validate(final DataTableSpec spec) throws InvalidSettingsException {
        for (final var p : m_predicates) {
            p.validate(spec);
        }
    }

    abstract boolean isSecondOutputActive();

    // UPDATE HANDLER

    /**
     * Compute possible enum values for filter operator based on the selected column.
     */
    static class TypeBasedOperatorsProvider implements EnumChoicesProvider<FilterOperator> {

        private Supplier<StringOrEnum<RowIdentifiers>> m_columnSelection;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
            m_columnSelection = initializer.computeFromValueSupplier(SelectedColumnRef.class);
        }

        @Override
        public List<FilterOperator> choices(final NodeParametersInput context) throws WidgetHandlerException {
            return getFilterOperators(context, m_columnSelection.get());
        }

        private static List<FilterOperator> getFilterOperators(final NodeParametersInput context,
            final StringOrEnum<RowIdentifiers> column) {

            final var rowIdentifierChoice = column.getEnumChoice();
            final var dataType = rowIdentifierChoice.map(TypeBasedOperatorsProvider::getColumnType)//
                .or(() -> context.getInTableSpec(0)//
                    .map(dts -> dts.getColumnSpec(column.getStringChoice()))//
                    .filter(Objects::nonNull)//
                    .map(DataColumnSpec::getType));

            if (dataType.isEmpty()) {
                // we don't know the column, but we know that columns always can contain missing cells
                return List.of(FilterOperator.IS_MISSING, FilterOperator.IS_NOT_MISSING);
            }
            // filter on top-level type
            return Arrays.stream(FilterOperator.values()) //
                .filter(op -> !op.isHidden(rowIdentifierChoice.orElse(null), dataType.get())) //
                .toList();
        }

        private static DataType getColumnType(final RowIdentifiers optionalSpecialColumn) {
            return switch (optionalSpecialColumn) {
                case ROW_ID -> StringCell.TYPE;
                case ROW_NUMBER -> LongCell.TYPE;
            };
        }
    }

    static class DefaultFilterCriterionProvider implements StateProvider<FilterCriterion> {

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeBeforeOpenDialog();
        }

        @Override
        public FilterCriterion computeState(final NodeParametersInput context) {
            final var filterCriterion = new FilterCriterion(context);
            final var validOperators = TypeBasedOperatorsProvider.getFilterOperators(context, filterCriterion.m_column);
            if (!validOperators.contains(FilterOperator.EQ)) {
                filterCriterion.m_operator = validOperators.get(0);
            }
            return filterCriterion;
        }
    }

    // UTILITIES

    static boolean hasLastNFilter(final List<FilterCriterion> criteria) {
        return criteria.stream().anyMatch(c -> c.m_operator == FilterOperator.LAST_N_ROWS);
    }

    // SECTIONS

    interface DialogSections {
        @Section(title = "Filter")
        interface Filter {
            interface AllAny {
            }

            interface Conditions {
            }
        }

        @Section(title = "Output")
        @After(Filter.class)
        interface Output {
            interface OutputMode {
            }

            interface Domain {
            }
        }
    }

    static IndexedRowReadPredicate createFilterPredicate(final boolean isAnd,
        final List<FilterCriterion> rowNumberCriteria, final List<FilterCriterion> dataCriteria,
        final DataTableSpec spec, final long tableSize) throws InvalidSettingsException {
        // TODO(performance): use domain bounds to derive whether predicates are always true or always false
        // TODO(performance): propagate ALWAYS_TRUE and ALWAYS_FALSE predicates
        final var optRowNumbers = mergeRowNumberPredicates(isAnd, mapToPredicates(rowNumberCriteria, tableSize));
        final var data = mergeValuePredicates(isAnd, mapToPredicates(dataCriteria, spec)).orElseThrow(
            () -> new IllegalStateException("Row number predicate without data predicate, should have used slicing"));
        if (optRowNumbers.isEmpty()) {
            return data;
        }
        final var rowNumbers = optRowNumbers.get();
        return isAnd ? (index, read) -> rowNumbers.test(index, read) && data.test(index, read) // NOSONAR this is not too hard to read
            : (index, read) -> rowNumbers.test(index, read) || data.test(index, read); // NOSONAR see above
    }

    private static List<IndexedRowReadPredicate> mapToPredicates(final List<FilterCriterion> criteria,
        final DataTableSpec spec) throws InvalidSettingsException {
        final var predicates = new ArrayList<IndexedRowReadPredicate>();
        for (final var filterCriterion : criteria) {
            predicates.add(filterCriterion.toPredicate(spec));
        }
        return predicates;
    }

    private static List<IndexedRowReadPredicate> mapToPredicates(final List<FilterCriterion> criteria,
        final long optionalTableSize) throws InvalidSettingsException {
        final var predicates = new ArrayList<IndexedRowReadPredicate>();
        for (final var filterCriterion : criteria) {
            final var filterSpec = RowNumberFilterSpec.toFilterSpec(filterCriterion);
            final var offsetFilter = filterSpec.toOffsetFilter(optionalTableSize);
            predicates.add(offsetFilter.asPredicate());
        }
        return predicates;
    }

    /* === Private methods operating on "core" classes  === */

    /**
     * Merges the given predicates using AND or OR, possibly short-circuiting.
     *
     * @param isAnd if {@code true}, combine predicates with AND, otherwise with OR
     * @param rowNumberCriteria list of predicates to merge
     * @return merged predicate
     */
    private static Optional<IndexedRowReadPredicate> mergeRowNumberPredicates(final boolean isAnd,
        final List<IndexedRowReadPredicate> rowNumberCriteria) {
        return rowNumberCriteria.stream().reduce((l, r) -> isAnd ? l.and(r) : l.or(r));
    }

    /**
     * Merges the given predicates using AND or OR.
     *
     * @param isAnd if {@code true}, combine predicates with AND, otherwise with OR
     * @param predicates list of predicates to merge
     * @return merged predicate, possibly short-circuited
     */
    private static Optional<IndexedRowReadPredicate> mergeValuePredicates(final boolean isAnd,
        final List<IndexedRowReadPredicate> predicates) {
        return predicates.stream().reduce((l, r) -> merge(isAnd, l, r));
    }

    /**
     * Merges the given predicates, short-circuiting if possible.
     *
     * @param isAnd if {@code true}, combine predicates with AND, otherwise with OR
     * @param l left-hand-side predicate
     * @param r right-hand-side predicate
     * @return combined predicate
     */
    private static final IndexedRowReadPredicate merge(final boolean isAnd, final IndexedRowReadPredicate l,
        final IndexedRowReadPredicate r) {
        return isAnd ? // NOSONAR
        // AND case
        // l AND false -> false
            (r == IndexedRowReadPredicate.FALSE ? IndexedRowReadPredicate.FALSE // NOSONAR
                // l AND true -> l
                : (r == IndexedRowReadPredicate.TRUE ? l // NOSONAR
                    // else simply combine
                    : l.and(r)))
            : // OR case
                           // l OR false -> l
            (r == IndexedRowReadPredicate.FALSE ? l // NOSONAR
                // x OR true -> true
                : (r == IndexedRowReadPredicate.TRUE ? IndexedRowReadPredicate.TRUE // NOSONAR
                    : // else simply combine
                    l.or(r)));
    }

}
