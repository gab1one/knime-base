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
 *   Aug 16, 2019 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.base.node.preproc.topk;

import java.util.Collections;
import java.util.List;

import org.knime.base.node.util.SortKeyItem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Manages the settings for the Top K Selector node.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class TopKSelectorSettings {

    /**
     * Sort key to sort by. Includes with each column/row key the sort order and type of string comparison.
     */
    private List<SortKeyItem> m_sortKey = Collections.emptyList();

    private final SettingsModelBoolean m_missingToEnd = new SettingsModelBoolean("missingsToEnd", true);

    private final SettingsModelString m_outputOrder = new SettingsModelString("outputOrder",
        OutputOrder.NO_ORDER.name());

    private final TopKModeSettingsModel m_topKMode = new TopKModeSettingsModel("selectionMode",
        TopKMode.TOP_K_ROWS.getText());

    private final SettingsModelIntegerBounded m_k = new SettingsModelIntegerBounded("k", 5, 1, Integer.MAX_VALUE);

    void saveSettingsTo(final NodeSettingsWO settings) {
        m_k.saveSettingsTo(settings);
        m_missingToEnd.saveSettingsTo(settings);
        m_outputOrder.saveSettingsTo(settings);
        m_topKMode.saveSettingsTo(settings);
        SortKeyItem.saveTo(m_sortKey, TopKSelectorNodeModel.INCLUDELIST_KEY, TopKSelectorNodeModel.SORTORDER_KEY,
            TopKSelectorNodeModel.ALPHANUMCOMP_KEY, settings);
    }

    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_k.validateSettings(settings);
        m_missingToEnd.validateSettings(settings);
        m_outputOrder.validateSettings(settings);
        m_topKMode.validateSettings(settings);
        SortKeyItem.validate(TopKSelectorNodeModel.INCLUDELIST_KEY, TopKSelectorNodeModel.SORTORDER_KEY,
            TopKSelectorNodeModel.ALPHANUMCOMP_KEY, settings);
    }

    void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_k.loadSettingsFrom(settings);
        m_missingToEnd.loadSettingsFrom(settings);
        m_outputOrder.loadSettingsFrom(settings);
        m_topKMode.loadSettingsFrom(settings);
        m_sortKey = SortKeyItem.loadFrom(TopKSelectorNodeModel.INCLUDELIST_KEY, TopKSelectorNodeModel.SORTORDER_KEY,
            TopKSelectorNodeModel.ALPHANUMCOMP_KEY, settings);
    }


    SettingsModelIntegerBounded getKModel() {
        return m_k;
    }

    SettingsModelString getOutputOrderModel() {
        return m_outputOrder;
    }

    SettingsModelString getTopKModeModel() {
        return m_topKMode;
    }

    SettingsModelBoolean getMissingToEndModel() {
        return m_missingToEnd;
    }

    public List<SortKeyItem> getSortKey() {
        return m_sortKey;
    }

    int getK() {
        return m_k.getIntValue();
    }

    boolean isMissingToEnd() {
        return m_missingToEnd.getBooleanValue();
    }

    OutputOrder getOutputOrder() {
        return OutputOrder.valueOf(m_outputOrder.getStringValue());
    }

    String getTopKMode() {
        return m_topKMode.getStringValue();
    }

    /**
     * Ensures backwards compatibility by the new introduced {@link TopKMode} option with AP-13006.
     *
     * @author Lars Schweikardt, KNIME GmbH, Konstanz, Germany
     */
    private static class TopKModeSettingsModel extends SettingsModelString {

        /**
         * Constructor.
         *
         * @param configName
         * @param defaultValue
         */
        public TopKModeSettingsModel(final String configName, final String defaultValue) {
            super(configName, defaultValue);
        }

        @Override
        protected void loadSettingsForDialog(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {
            setStringValue(settings.getString(getKey(), TopKMode.TOP_K_ROWS.getText()));
        }

        @Override
        protected void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
            setStringValue(settings.getString(getKey(), TopKMode.TOP_K_ROWS.getText()));
        }

        @Override
        protected void validateSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String key = getKey();
            if (settings.containsKey(key)) {
                final String selectionMode = settings.getString(key);
                try {
                    TopKMode.getTopKModeByText(selectionMode);
                } catch (IllegalArgumentException e) {
                    throw new InvalidSettingsException(String.format(
                        "No selection mode was found for the input \"%s\". Change it in the configuration.",
                        selectionMode), e);
                }
                super.validateSettingsForModel(settings);
            }
        }
    }
}
