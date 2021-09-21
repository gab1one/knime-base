/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   23.10.2013 (gabor): created
 */
package org.knime.base.node.mine.scorer.numeric2;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;

/**
 * <code>NodeDialog</code> for the "NumericScorer" Node. Computes the distance between the a numeric column's values and
 * predicted values.
 *
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows creation of a simple dialog with standard
 * components. If you need a more complex dialog please derive directly from {@link org.knime.core.node.NodeDialogPane}.
 *
 * @author Gabor Bakos
 * @author Eric Axt
 * @since 4.0
 */
public class NumericScorer2NodeDialog extends DefaultNodeSettingsPane {

    private final NumericScorer2Settings m_settings = new NumericScorer2Settings();

    private final NumericScorer2DialogComponents m_components = new NumericScorer2DialogComponents(m_settings);

    /**
     * New pane for configuring the NumericScorer node.
     */
    protected NumericScorer2NodeDialog() {
        addDialogComponent(m_components.getReferenceComponent());
        addDialogComponent(m_components.getPredictionComponent());
        createNewGroup("Output column");
        addDialogComponent(m_components.getOverrideComponent());
        addDialogComponent(m_components.getOutputComponent());

        createNewGroup("Provide scores as flow variables");
        addDialogComponent(m_components.getUseNamePrefixComponent());
        addDialogComponent(m_components.getFlowVarComponent());

        createNewGroup("Adjusted R squared");
        addDialogComponent(m_components.getAdjustedRSquare());


    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onOpen() {
        super.onOpen();
        m_settings.onOpen();
    }
}
