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
 *   27 Aug 2024 (Manuel Hotz, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.base.node.preproc.filter.row3.predicates;

import java.util.Optional;
import java.util.OptionalInt;

import org.knime.base.data.filter.row.v2.IndexedRowReadPredicate;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.dynamic.DynamicValuesInput;

/**
 * Predicate factory for {@link BooleanValue} columns, i.e. column data types that have
 * {@link BooleanValue} as preferred value class.
 *
 * @author Manuel Hotz, KNIME GmbH, Konstanz, Germany
 */
final class BooleanPredicateFactory extends AbstractPredicateFactory {

    private final boolean m_matchTrue;

    private BooleanPredicateFactory(final boolean matchTrue) {
        m_matchTrue = matchTrue;
    }

    /**
     * Creates a factory for boolean predicates, if the column data type prefers {@link BooleanValue}.
     *
     * @param columnDataType the data type of the column
     * @param matchTrue whether to match true or false
     * @return an optional predicate factory
     */
    static Optional<PredicateFactory> create(final DataType columnDataType, final boolean matchTrue) {
        if (BooleanValue.class.equals(columnDataType.getPreferredValueClass())) {
            return Optional.of(new BooleanPredicateFactory(matchTrue));
        }
        return Optional.empty();
    }

    @Override
    public IndexedRowReadPredicate createPredicate(final OptionalInt colIdx, final DynamicValuesInput ignored)
        throws InvalidSettingsException {
        final var columnIndex = colIdx.orElseThrow(
            () -> new IllegalStateException("Boolean predicate operates on column but did not get a column index"));
        return (i, rowRead) -> rowRead.<BooleanValue> getValue(columnIndex).getBooleanValue() == m_matchTrue;
    }

}
