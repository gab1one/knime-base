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
 *   31 Mar 2024 (jasper): created
 */
package org.knime.base.node.preproc.filter.row3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.knime.base.node.preproc.filter.row3.AbstractRowFilterNodeSettings.TypeBasedOperatorsProvider;
import org.knime.base.node.preproc.filter.row3.operators.RowNumberFilterSpec;
import org.knime.base.node.preproc.filter.row3.operators.legacy.DynamicValuesInput;
import org.knime.base.node.preproc.filter.row3.operators.legacy.LegacyFilterOperator;
import org.knime.base.node.preproc.filter.row3.predicates.PredicateFactories;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTableSpecCreator;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.webui.node.dialog.defaultdialog.NodeParametersUtil;
import org.knime.core.webui.node.dialog.defaultdialog.setting.singleselection.StringOrEnum;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.updates.ButtonReference;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;

/**
 * Tests for FilterOperator choices.
 *
 * @author Jasper Krauter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"restriction", "static-method"})
@ExtendWith(FilterDummyDataCellExtension.class)
final class FilterOperatorTest {

    private static final DataTableSpec SPEC = new DataTableSpecCreator() //
        .addColumns( //
            new DataColumnSpecCreator("Int1", IntCell.TYPE).createSpec(), //
            new DataColumnSpecCreator("Double1", DoubleCell.TYPE).createSpec(), //
            new DataColumnSpecCreator("Bool1", BooleanCell.TYPE).createSpec(), //
            new DataColumnSpecCreator("String1", StringCell.TYPE).createSpec(), //
            new DataColumnSpecCreator("Int2", IntCell.TYPE).createSpec(), //
            new DataColumnSpecCreator("Long1", LongCell.TYPE).createSpec()) //
        .createSpec();

    @Test
    void testOperatorChoices() {
        assertThat(operatorChoicesFor("String1")).as("The list of operators for a string column is what is expected")
            .containsExactlyInAnyOrder( //
                LegacyFilterOperator.IS_MISSING, //
                LegacyFilterOperator.IS_NOT_MISSING, //
                LegacyFilterOperator.EQ, //
                LegacyFilterOperator.NEQ, //
                LegacyFilterOperator.NEQ_MISS, //
                LegacyFilterOperator.REGEX, //
                LegacyFilterOperator.WILDCARD //
            );
        assertThat(operatorChoicesFor("Int1")).as("The list of operators for an integer column is what is expected")
            .containsExactlyInAnyOrder( //
                LegacyFilterOperator.IS_MISSING, //
                LegacyFilterOperator.IS_NOT_MISSING, //
                LegacyFilterOperator.EQ, //
                LegacyFilterOperator.NEQ, //
                LegacyFilterOperator.NEQ_MISS, //
                LegacyFilterOperator.GT, //
                LegacyFilterOperator.GTE, //
                LegacyFilterOperator.LT, //
                LegacyFilterOperator.LTE, //
                LegacyFilterOperator.WILDCARD, //
                LegacyFilterOperator.REGEX //
            );
        assertThat(operatorChoicesFor("Long1")).as("The list of operators for int cells is the same as for long cells")
            .isEqualTo(operatorChoicesFor("Int1"));
        assertThat(operatorChoicesFor("Double1")).as("The list of operators for a double column is what is expected")
            .containsExactlyInAnyOrder( //
                LegacyFilterOperator.IS_MISSING, //
                LegacyFilterOperator.IS_NOT_MISSING, //
                LegacyFilterOperator.EQ, //
                LegacyFilterOperator.NEQ, //
                LegacyFilterOperator.NEQ_MISS, //
                LegacyFilterOperator.GT, //
                LegacyFilterOperator.GTE, //
                LegacyFilterOperator.LT, //
                LegacyFilterOperator.LTE);
        assertThat(operatorChoicesFor("Bool1")).as("The list of operators for a boolean column is what is expected")
            .containsExactlyInAnyOrder( //
                LegacyFilterOperator.IS_MISSING, //
                LegacyFilterOperator.IS_NOT_MISSING, //
                LegacyFilterOperator.IS_TRUE, //
                LegacyFilterOperator.IS_FALSE //
            );
        assertThat(operatorChoicesFor("Unknown Column"))
            .as("The list of operators for an unknown column type is what is expected").containsExactlyInAnyOrder( //
                LegacyFilterOperator.IS_MISSING, //
                LegacyFilterOperator.IS_NOT_MISSING //
            );
    }

    @Test
    void testOperatorChoicesForSpecialColumns() {
        assertThat(operatorChoicesFor(RowIdentifiers.ROW_NUMBER))
            .as("The list of operators for the row numbers is what is expected").containsExactlyInAnyOrder( //
                LegacyFilterOperator.EQ, //
                LegacyFilterOperator.NEQ, //
                LegacyFilterOperator.GT, //
                LegacyFilterOperator.GTE, //
                LegacyFilterOperator.LT, //
                LegacyFilterOperator.LTE, //
                LegacyFilterOperator.LAST_N_ROWS, //
                LegacyFilterOperator.FIRST_N_ROWS, //
                LegacyFilterOperator.WILDCARD, //
                LegacyFilterOperator.REGEX //
            );
        assertThat(operatorChoicesFor(RowIdentifiers.ROW_ID))
            .as("The list of operators for the row id column is what is expected").containsExactlyInAnyOrder( //
                LegacyFilterOperator.EQ, //
                LegacyFilterOperator.NEQ, //
                LegacyFilterOperator.REGEX, //
                LegacyFilterOperator.WILDCARD //
            );
    }

    static LegacyFilterOperator[] operatorChoicesFor(final String col) {
        return operatorChoicesFor(new StringOrEnum<>(col));
    }

    static LegacyFilterOperator[] operatorChoicesFor(final RowIdentifiers col) {
        return operatorChoicesFor(new StringOrEnum<>(col));
    }

    static LegacyFilterOperator[] operatorChoicesFor(final StringOrEnum<RowIdentifiers> columnSelection) {
        final var ctx = NodeParametersUtil.createDefaultNodeSettingsContext(new DataTableSpec[]{SPEC});

        final var provider = new TypeBasedOperatorsProvider();
        provider.init(new TestInitializer() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> Supplier<T> computeFromValueSupplier(final Class<? extends ParameterReference<T>> ref) {
                if (ref.equals(AbstractRowFilterNodeSettings.FilterCriterion.SelectedColumnRef.class)) {
                    return () -> (T)columnSelection;
                }
                throw new IllegalStateException("Unexpected dependency \"%s\"".formatted(ref.getName()));
            }

            @Override
            public void computeAfterOpenDialog() {
                // Do nothing
            }

        });

        return provider.choices(ctx).toArray(LegacyFilterOperator[]::new);
    }

    static class TestInitializer implements StateProvider.StateProviderInitializer {

        @Override
        public <T> Supplier<T> computeFromValueSupplier(final Class<? extends ParameterReference<T>> ref) {
            throw new IllegalStateException("Not expected to be called during test.");
        }

        @Override
        public <T> Supplier<T> getValueSupplier(final Class<? extends ParameterReference<T>> ref) {
            throw new IllegalStateException("Not expected to be called during test.");
        }

        @Override
        public <T> void computeOnValueChange(final Class<? extends ParameterReference<T>> id) {
            fail("Not expected to be called during test.");
        }

        @Override
        public <T> Supplier<T> computeFromProvidedState(final Class<? extends StateProvider<T>> stateProviderClass) {
            throw new IllegalStateException("Not expected to be called during test.");
        }

        @Override
        public void computeOnButtonClick(final Class<? extends ButtonReference> ref) {
            fail("Not expected to be called during test.");
        }

        @Override
        public void computeBeforeOpenDialog() {
            throw new IllegalStateException("Not expected to be called during test.");
        }

        @Override
        public void computeAfterOpenDialog() {
            fail("Not expected to be called during test.");
        }

        @Override
        public NodeParametersInput getNodeParametersInput() {
            throw new IllegalStateException("Not expected to be called during test.");
        }
    }

    @ParameterizedTest
    @EnumSource(names = {"IS_MISSING", "IS_NOT_MISSING", "FIRST_N_ROWS", "LAST_N_ROWS"})
    void testOperatorsWithoutPredicateFactory(final LegacyFilterOperator operator) {
        assertThat(PredicateFactories.getValuePredicateFactory(operator, null))
            .as("Operator %s has no value predicate factory".formatted(operator)).isEmpty();
    }

    private abstract static class BaseTester {

        abstract Stream<LegacyFilterOperator> getOperators();

        final boolean test(final RowIdentifiers specialColumn, final DataType type) {
            // tests that the operator is not hidden and can be applied (i.e. has a predicate factory)
            return getOperators() //
                .map(op -> !op.isHidden(specialColumn, type) && op.isApplicableFor(specialColumn, type))
                .allMatch(b -> b);
        }
    }

    private static final class PatternMatchable extends BaseTester {
        @Override
        Stream<LegacyFilterOperator> getOperators() {
            return Stream.of(LegacyFilterOperator.REGEX, LegacyFilterOperator.WILDCARD);
        }
    }

    @Test
    void testIsPatternMatchable() {
        final var tester = new PatternMatchable();
        assertThat(tester) //
            .as("RowID is pattern-matchable") //
            .returns(true, t -> t.test(RowIdentifiers.ROW_ID, StringCell.TYPE)) //
            .as("Normal string column is pattern-matchable") //
            .returns(true, t -> t.test(null, StringCell.TYPE)) //
            .as("Long column is pattern-matchable") //
            .returns(true, t -> t.test(null, LongCell.TYPE)) //
            .as("Int column is pattern-matchable") //
            .returns(true, t -> t.test(null, IntCell.TYPE)) //
            .as("Boolean column is not pattern-matchable") //
            .returns(false, t -> t.test(null, BooleanCell.TYPE));

        // the pattern-match operators should require a String input type
        tester.getOperators().allMatch(op -> op.getRequiredInputType().map(t -> t == StringCell.TYPE).orElse(false));
    }

    private static final class IsEq extends BaseTester {
        @Override
        Stream<LegacyFilterOperator> getOperators() {
            return Stream.of(LegacyFilterOperator.EQ, LegacyFilterOperator.NEQ);
        }
    }

    @Test
    void testIsEq() {
        final var tester = new IsEq();
        assertThat(tester) //
            .as("RowID is eq-able") //
            .returns(true, t -> t.test(RowIdentifiers.ROW_ID, StringCell.TYPE)) //
            .as("Normal string column is eq-able") //
            .returns(true, t -> t.test(null, StringCell.TYPE)) //
            .as("Long column is eq-able") //
            .returns(true, t -> t.test(null, LongCell.TYPE)) //
            .as("Int column is eq-able") //
            .returns(true, t -> t.test(null, IntCell.TYPE)) //
            .as("Double column is eq-able") //
            .returns(true, t -> t.test(null, DoubleCell.TYPE)) //
            .as("Boolean column is not eq-able, should use dedicated operators for that") //
            .returns(false, t -> t.test(null, BooleanCell.TYPE))
            .as("Custom type column is eq-able, if type-mapping has String->Cell converter") //
            .returns(true, t -> t.test(null, FilterDummyDataCellExtension.FilterDummyCell.TYPE));
    }

    private static final class IsTruthy extends BaseTester {

        @Override
        Stream<LegacyFilterOperator> getOperators() {
            return Stream.of(LegacyFilterOperator.IS_TRUE, LegacyFilterOperator.IS_FALSE);
        }
    }

    @Test
    void testIsTruthy() {
        final var tester = new IsTruthy();
        assertThat(tester) //
            .as("RowID is not truthy") //
            .returns(false, t -> t.test(RowIdentifiers.ROW_ID, StringCell.TYPE)) //
            .as("Normal string column is not truthy") //
            .returns(false, t -> t.test(null, StringCell.TYPE)) //
            .as("Long column is not truthy") //
            .returns(false, t -> t.test(null, LongCell.TYPE)) //
            .as("Int column is not truthy") //
            .returns(false, t -> t.test(null, IntCell.TYPE)) //
            .as("Double column is not truthy") //
            .returns(false, t -> t.test(null, DoubleCell.TYPE)) //
            .as("Boolean column is truthy") //
            .returns(true, t -> t.test(null, BooleanCell.TYPE));
    }

    private static final class IsRowNumber extends BaseTester {

        private static final LegacyFilterOperator[] SLICED_OPS =
            {LegacyFilterOperator.EQ, LegacyFilterOperator.NEQ, LegacyFilterOperator.GT, LegacyFilterOperator.GTE, LegacyFilterOperator.LT,
                LegacyFilterOperator.LTE, LegacyFilterOperator.FIRST_N_ROWS, LegacyFilterOperator.LAST_N_ROWS};

        private static final LegacyFilterOperator[] VALUE_OPS = {LegacyFilterOperator.WILDCARD, LegacyFilterOperator.REGEX};

        @Override
        Stream<LegacyFilterOperator> getOperators() {
            return Stream.concat(Arrays.stream(SLICED_OPS), Arrays.stream(VALUE_OPS));
        }

        void validate(final DynamicValuesInput input) throws InvalidSettingsException {
            final var criterion = new AbstractRowFilterNodeSettings.FilterCriterion();
            criterion.m_column = new StringOrEnum<>(RowIdentifiers.ROW_NUMBER);
            criterion.m_predicateValues = input;
            for (final var op : SLICED_OPS) {
                criterion.m_operator = op;
                RowNumberFilterSpec.toFilterSpec(criterion);
            }
            for (final var op : VALUE_OPS) {
                criterion.m_operator = op;
                assertThat(PredicateFactories.getRowNumberPredicateFactory(op))
                    .as("Missing factory for operator \"%s\"", op).isPresent();
            }
        }
    }

    @Test
    void testIsRowNumber() {
        final var tester = new IsRowNumber();
        assertThat(tester) //
            .as("Row number is row number") //
            .returns(true, t -> t.test(RowIdentifiers.ROW_NUMBER, IntCell.TYPE)) //
            .as("RowID is not row number") //
            .returns(false, t -> t.test(RowIdentifiers.ROW_ID, StringCell.TYPE)) //
            .as("Normal string column is not row number").returns(false, t -> t.test(null, StringCell.TYPE)); //

        assertThatCode(() -> tester.validate(DynamicValuesInput.forRowNumber(LongCell.TYPE))) //
            .as("Row number with default value validates") //
            .doesNotThrowAnyException();
    }

    private static final class IsOrd extends BaseTester {

        @Override
        Stream<LegacyFilterOperator> getOperators() {
            return Stream.of(LegacyFilterOperator.GT, LegacyFilterOperator.GTE, LegacyFilterOperator.LT, LegacyFilterOperator.LTE);
        }

    }

    @Test
    void testIsOrd() {
        final var tester = new IsOrd();
        // assert that long is ord but string and boolean are not ord
        assertThat(tester) //
            .as("Long cell is Ord") //
            .returns(true, t -> t.test(null, LongCell.TYPE)) //
            .as("Boolean cell is not Ord") //
            .returns(false, t -> t.test(null, BooleanCell.TYPE)) //
            .as("String cell is not Ord)") //
            .returns(false, t -> t.test(null, StringCell.TYPE)) //
            .as("RowID is not Ord") //
            .returns(false, t -> t.test(RowIdentifiers.ROW_ID, StringCell.TYPE)) //
        ;
    }

    private static final class CanBeMissing extends BaseTester {
        @Override
        Stream<LegacyFilterOperator> getOperators() {
            return Stream.of(LegacyFilterOperator.IS_MISSING, LegacyFilterOperator.IS_NOT_MISSING);
        }
    }

    @Test
    void testCanBeMissing() {
        // assert that anything except RowID and RowNumber can be missing
        final var tester = new CanBeMissing();
        assertThat(tester) //
            .as("Long cell can be missing") //
            .returns(true, t -> t.test(null, LongCell.TYPE)) //
            .as("String cell can be missing") //
            .returns(true, t -> t.test(null, StringCell.TYPE)) //
            .as("Boolean cell can be missing") //
            .returns(true, t -> t.test(null, BooleanCell.TYPE)) //
            .as("RowID cannot be missing") //
            .returns(false, t -> t.test(RowIdentifiers.ROW_ID, StringCell.TYPE)) //
            .as("RowNumber cannot be missing") //
            .returns(false, t -> t.test(RowIdentifiers.ROW_NUMBER, IntCell.TYPE));
    }

    private static final class IsNeqMiss extends BaseTester {
        @Override
        Stream<LegacyFilterOperator> getOperators() {
            return Stream.of(LegacyFilterOperator.NEQ_MISS);
        }
    }

    @Test
    void testIsNeqMiss() {
        final var tester = new IsNeqMiss();
        assertThat(tester) //
            .as("RowID is not NEQ_MISS") // because it cannot be missing
            .returns(false, t -> t.test(RowIdentifiers.ROW_ID, StringCell.TYPE)) //
            .as("Row number is not NEQ_MISS") // because it cannot be missing
            .returns(false, t -> t.test(RowIdentifiers.ROW_NUMBER, LongCell.TYPE)) //
            .as("Normal string column is NEQ_MISS") //
            .returns(true, t -> t.test(null, StringCell.TYPE)) //
            .as("Long column is NEQ_MISS") //
            .returns(true, t -> t.test(null, LongCell.TYPE)) //
            .as("Int column is NEQ_MISS") //
            .returns(true, t -> t.test(null, IntCell.TYPE)) //
            .as("Double column is NEQ_MISS") //
            .returns(true, t -> t.test(null, DoubleCell.TYPE)) //
            .as("Boolean column is not NEQ_MISS") // because it is not NEQ
            .returns(false, t -> t.test(null, BooleanCell.TYPE));
    }
}
