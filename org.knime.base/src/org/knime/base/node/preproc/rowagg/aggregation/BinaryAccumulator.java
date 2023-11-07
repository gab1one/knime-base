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
 *   31 Oct 2023 (Manuel Hotz, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.base.node.preproc.rowagg.aggregation;

import java.util.Optional;

import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;

/**
 * Stateful accumulator of {@link DataValue data values} whose result type is a function of the two input data types.
 *
 * @param <V> first input type
 * @param <W> second input type
 * @param <O> accumulation result type
 *
 * @author Manuel Hotz, KNIME GmbH, Konstanz, Germany
 * @noreference This interface is not intended to be referenced by clients.
 */
public interface BinaryAccumulator<V extends DataValue, W extends DataValue, O extends DataValue> {

    /**
     * Compute current aggregate based on given values. The method can indicate if the aggregate reached a "fixed
     * point", i.e. the computed aggregate will not change anymore based on any possible input (e.g. if overflows should
     * short-circuit the aggregation or not).
     *
     * If the data values involved are numeric, the method may also indicate that the domain over/underflowed by
     * throwing an instance of {@link ArithmeticException} with an error message.
     *
     * @param v first data value
     * @param w second data value
     * @return {@code true} if the aggregate reached a "fixed point" and the result will not change, {@code false} if it
     *         still <i>could</i> change
     * @throws ArithmeticException when a numeric over/underflow occurs
     */
    boolean apply(V v, W w) throws ArithmeticException;

    /**
     * Result type of aggregate.
     *
     * @return result type
     */
    DataType getResultType();

    /**
     * Current result of the aggregate.
     *
     * @return result of aggregate, empty if error value (e.g. overflow)
     */
    Optional<O> getResult();

    /**
     * Resets the internal state of the accumulator.
     */
    void reset();

}
