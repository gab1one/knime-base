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
 *   16 Dec 2024 (Manuel Hotz, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.base.node.preproc.filter.row3.operators;

import java.util.List;
import java.util.function.LongFunction;

import org.knime.base.data.filter.row.v2.FilterPartition;
import org.knime.base.data.filter.row.v2.OffsetFilter;
import org.knime.base.data.filter.row.v2.OffsetFilter.Operator;
import org.knime.base.node.preproc.filter.row3.FilterMode;
import org.knime.base.node.preproc.filter.row3.operators.legacy.LegacyFilterOperator;
import org.knime.base.node.preproc.filter.row3.operators.legacy.LegacyFilterParameters;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.webui.node.dialog.defaultdialog.internal.dynamic.extensions.filtervalue.FilterValueParameters;

/**
 * Subset of filter operators that represent a numeric filter on the row number.
 */
public final class RowNumberFilterSpec {

    static final long UNKNOWN_SIZE = -1;

    private final LegacyFilterOperator m_operator;

    private final long m_value;

    RowNumberFilterSpec(final LegacyFilterOperator operator, final long value) throws InvalidSettingsException {
        CheckUtils.checkSetting(supportsOperator(operator), "Cannot use operator \"%s\" to filter by row number",
            operator);
        m_operator = operator;
        m_value = value;
    }

    static boolean supportsOperator(final LegacyFilterOperator operator) {
        return switch (operator) {
            case EQ, NEQ, NEQ_MISS, LT, LTE, GT, GTE, FIRST_N_ROWS, LAST_N_ROWS -> true;
            case IS_FALSE, IS_TRUE, IS_MISSING, IS_NOT_MISSING, REGEX, WILDCARD -> false;
        };
    }

    /**
     * Computes the index operator and offset given the filter operator and value.
     *
     * @param optionalTableSize table size if known
     * @return index operator and index value (non-negative offset)
     */
    public OffsetFilter toOffsetFilter(final long optionalTableSize) {
        // the dialog accepts 1-based row numbers but we use 0-based row offsets internally
        return switch (m_operator) {
            case EQ -> new OffsetFilter(Operator.EQ, m_value - 1);
            case NEQ, NEQ_MISS -> new OffsetFilter(Operator.NEQ, m_value - 1);
            case LT -> new OffsetFilter(Operator.LT, m_value - 1);
            case LTE -> new OffsetFilter(Operator.LTE, m_value - 1);
            case GT -> new OffsetFilter(Operator.GT, m_value - 1);
            case GTE -> new OffsetFilter(Operator.GTE, m_value - 1);
            case FIRST_N_ROWS -> new OffsetFilter(Operator.LT, m_value);
            case LAST_N_ROWS -> {
                CheckUtils.checkState(optionalTableSize != UNKNOWN_SIZE, //
                    "Expected table size for filter operator \"%s\"", m_operator);
                // if the table has fewer than `n` rows, return the whole table
                yield new OffsetFilter(Operator.GTE, Math.max(0, optionalTableSize - m_value));
            }
            // not supported
            case IS_FALSE, IS_TRUE, IS_MISSING, IS_NOT_MISSING, REGEX, WILDCARD -> throw new IllegalStateException();
        };
    }

    public static FilterPartition computeRowPartition(final boolean isAnd,
        final List<LongFunction<FilterPartition>> rowNumberFilters, final FilterMode outputMode,
        final long optionalTableSize) {
        final var matchedNonMatchedPartition = rowNumberFilters.stream() //
            .map(createPartition -> createPartition.apply(optionalTableSize)) //
            .reduce((a, b) -> a.combine(isAnd, b)) //
            .orElseThrow(() -> new IllegalArgumentException("Need at least one filter criterion"));
        // determine whether matched or non-matched rows are included in the first output, flip pair as needed
        return outputMode == FilterMode.MATCHING ? matchedNonMatchedPartition : matchedNonMatchedPartition.swapped();
    }

    /**
     * If supported, get the criterion as a row number filter specification.
     *
     * @param operatorId the operator id
     * @param filterValueParameters suitable parameters
     *
     * @return row number filter specification
     * @throws InvalidSettingsException if the filter criterion contains an unsupported operator or the value is missing
     */
    public static LongFunction<FilterPartition> toFilterSpec(final String operatorId,
        final FilterValueParameters filterValueParameters) throws InvalidSettingsException {
        if (filterValueParameters instanceof LegacyFilterParameters legacyParameters) {
            final var rowNumberFilterSpec = legacyParameters.toFilterSpec();
            return tableSize -> FilterPartition.computePartition(rowNumberFilterSpec.toOffsetFilter(tableSize),
                tableSize);
        }
        final var matchingOperator =
            FilterOperatorsUtil.findMatchingRowNumberOperator(operatorId).orElseThrow(IllegalStateException::new);
        return toSliceFilter(matchingOperator, filterValueParameters);
    }

    static <P extends FilterValueParameters> LongFunction<FilterPartition> toSliceFilter(
        final RowNumberFilterOperator<P> matchingOperator, final FilterValueParameters filterValueParameters)
        throws InvalidSettingsException {
        return matchingOperator.createSliceFilter((P)filterValueParameters);
    }

}
