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
 * ---------------------------------------------------------------------
 *
 * History
 *   27.07.2007 (thor): created
 */
package org.knime.base.node.preproc.joiner3;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

import javax.imageio.ImageIO;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingConstants;
import javax.swing.event.ChangeListener;

import org.knime.base.node.preproc.joiner3.Joiner3Settings.ColumnNameDisambiguationButtonGroup;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.CompositionModeButtonGroup;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.DataCellComparisonModeButtonGroup;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.RowKeyFactoryButtonGroup;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.join.JoinTableSettings.JoinColumn;
import org.knime.core.data.join.JoinTableSettings.SpecialJoinColumn;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.util.ColumnPairsSelectionPanel;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;

/**
 * This is the dialog for the joiner node.
 *
 * @author Martin Horn, KNIME GmbH, Konstanz, Germany
 */
class Joiner3NodeDialog extends NodeDialogPane {

    private static final String ICON_PATH = "org/knime/base/node/preproc/joiner3/vennDiagrams";

    private final Joiner3Settings m_settings = new Joiner3Settings();

    // join conditions
    private ColumnPairsSelectionPanel m_columnPairs;
    private final DialogComponentButtonGroup m_compositionMode;
    private final DialogComponentButtonGroup m_dataCellComparisonMode;

    // include in output
    private final DialogComponentBoolean m_includeMatchingRows;
    private final DialogComponentBoolean m_includeLeftUnmatchedRows;
    private final DialogComponentBoolean m_includeRightUnmatchedRows;

    // output options
    private final DialogComponentBoolean m_mergeJoinColumns;
    private final DialogComponentBoolean m_outputUnmatchedRowsToSeparatePorts;
    private final DialogComponentBoolean m_hilitingEnabled;

    // row keys
    private final DialogComponentButtonGroup m_rowKeyFactory;
    private final DialogComponentString m_rowKeySeparator;

    // include column selection and column name disambiguation
    private final DataColumnSpecFilterPanel m_leftFilterPanel = new DataColumnSpecFilterPanel();
    private final DataColumnSpecFilterPanel m_rightFilterPanel = new DataColumnSpecFilterPanel();
    private final DialogComponentString m_columnDisambiguationSuffix;
    private final DialogComponentButtonGroup m_columnDisambiguation;

    // performance
    private final DialogComponentButtonGroup m_outputRowOrder;
    private final DialogComponentNumberEdit m_maxOpenFiles;

    /**
     * Creates a new dialog for the joiner node.
     * @param settings
     *
     * @param settings
     */
    Joiner3NodeDialog() {

        m_compositionMode = new DialogComponentButtonGroup(m_settings.m_compositionModeModel, "Composition mode",
            true, Joiner3Settings.CompositionModeButtonGroup.values());

        m_dataCellComparisonMode = new DialogComponentButtonGroup(m_settings.m_dataCellComparisonModeModel,
            "Data cell comparison mode", true, Joiner3Settings.DataCellComparisonModeButtonGroup.values());

        // include in output
        m_includeMatchingRows = new DialogComponentBoolean(m_settings.m_includeMatchesModel, "Matching rows");
        m_includeMatchingRows.setToolTipText("Include rows that agree on the selected column pairs.");

        m_includeLeftUnmatchedRows =
            new DialogComponentBoolean(m_settings.m_includeLeftUnmatchedModel, "Left unmatched rows");
        m_includeLeftUnmatchedRows.setToolTipText(
            "Include rows from the left input table for which no matching row in the right input table is found.");

        m_includeRightUnmatchedRows =
            new DialogComponentBoolean(m_settings.m_includeRightUnmatchedModel, "Right unmatched rows");
        m_includeRightUnmatchedRows.setToolTipText(
                "Include rows from the right input table for which no matching row in the left input table is found.");

        // output options
        m_outputUnmatchedRowsToSeparatePorts = new DialogComponentBoolean(
            m_settings.m_outputUnmatchedRowsToSeparatePortsModel, "Split join result into multiple tables (top = matching rows, middle = left unmatched rows, bottom = right unmatched rows)");
        m_outputUnmatchedRowsToSeparatePorts.setToolTipText("Output unmatched rows (if selected under \"Include in"
            + " output\") at the second and third output port.");

        m_mergeJoinColumns = new DialogComponentBoolean(m_settings.m_mergeJoinColumnsModel, "Merge join columns");
        m_mergeJoinColumns
            .setToolTipText("Combine join columns with identical values into one column (see node description).");

        m_hilitingEnabled = new DialogComponentBoolean(m_settings.m_enableHilitingModel, "Hiliting enabled");
        m_hilitingEnabled.setToolTipText("Track which output rows have been produced by which input rows.");

        // row keys
        m_rowKeyFactory = new DialogComponentButtonGroup(m_settings.m_rowKeyFactoryModel, "Row keys of the output rows",
            true, Joiner3Settings.RowKeyFactoryButtonGroup.values());
        m_rowKeySeparator = new DialogComponentString(m_settings.m_rowKeySeparatorModel, "Separator");
        // enable row key separator input field only when concat is selected
        m_settings.m_rowKeyFactoryModel.addChangeListener(l -> m_settings.m_rowKeySeparatorModel
            .setEnabled(m_settings.getRowKeyFactory() == RowKeyFactoryButtonGroup.CONCATENATE));

        // include column selection and column name disambiguation
        m_columnDisambiguation = new DialogComponentButtonGroup(m_settings.m_columnDisambiguationModel,
            "Duplicate column handling", true, Joiner3Settings.ColumnNameDisambiguationButtonGroup.values());
        m_columnDisambiguationSuffix = new DialogComponentString(m_settings.m_columnNameSuffixModel, "Suffix");
        m_settings.m_columnDisambiguationModel.addChangeListener(l -> m_settings.m_columnNameSuffixModel
            .setEnabled(m_settings.getColumnNameDisambiguation() == ColumnNameDisambiguationButtonGroup.APPEND_SUFFIX));

        // performance
        m_outputRowOrder= new DialogComponentButtonGroup(m_settings.m_outputRowOrderModel,
            "Output row order", true, Joiner3Settings.OutputRowOrderButtonGroup.values());

        m_maxOpenFiles =
                new DialogComponentNumberEdit(m_settings.m_maxOpenFilesModel, "Maximum number of temporary files");

        addTab("Joiner Settings", createJoinerSettingsTab());
        addTab("Column Selection", createColumnSelectionTab());
        addTab("Performance", createPerformanceTab());
    }

    private JPanel createJoinerSettingsTab() {
        JPanel p = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();

        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTHWEST;
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;
        c.gridwidth = 1;
        c.weighty = 0;
        p.add(createJoinColumnsPanel(), c);

        c.gridy++;
        p.add(createJoinModePanel(), c);

        c.gridy++;
        p.add(createOutputPanel(), c);

        c.gridy++;
        p.add(createRowKeysPanel(), c);

        c.gridy++;
        c.weightx = 100;
        c.weighty = 100;
        c.fill = GridBagConstraints.BOTH;
        p.add(new JPanel(), c);

        return p;
    }

    private JPanel createJoinModePanel() {
        JPanel p = new JPanel(new BorderLayout());
        p.setBorder(BorderFactory.createTitledBorder("Include in output"));

        JPanel checkboxes = new JPanel(new GridLayout(3, 1));
        checkboxes.add(m_includeMatchingRows.getComponentPanel().getComponent(0));
        checkboxes.add(m_includeLeftUnmatchedRows.getComponentPanel().getComponent(0));
        checkboxes.add(m_includeRightUnmatchedRows.getComponentPanel().getComponent(0));

        JPanel right = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        JPanel joinModePanel = new JPanel(new FlowLayout(FlowLayout.CENTER, 10, 10));

        JLabel joinModeName = new JLabel("");
        joinModeName.setOpaque(false);
        joinModeName.setVerticalTextPosition(SwingConstants.TOP);
        joinModeName.setHorizontalTextPosition(SwingConstants.CENTER);

        joinModePanel.add(joinModeName);
        right.add(joinModePanel, c);
        ChangeListener joinModeListener = e -> {
            try {
                // display join name
                joinModeName.setText(m_settings.getJoinMode().toString());
                // set venn diagram image
                String diagramPath = String.format("%s/%s.png", ICON_PATH, m_settings.getJoinMode().name());
                URL url = Joiner3NodeDialog.class.getClassLoader().getResource(diagramPath);
                joinModeName.setIcon(new ImageIcon(ImageIO.read(url)));
            } catch (IOException e1) {
                getLogger().warn(e1);
            }
        };
        m_settings.m_includeMatchesModel.addChangeListener(joinModeListener);
        m_settings.m_includeLeftUnmatchedModel.addChangeListener(joinModeListener);
        m_settings.m_includeRightUnmatchedModel.addChangeListener(joinModeListener);

        // toggle once to fire event handler
        m_settings.m_includeMatchesModel.setBooleanValue(!m_settings.m_includeMatchesModel.getBooleanValue());
        m_settings.m_includeMatchesModel.setBooleanValue(!m_settings.m_includeMatchesModel.getBooleanValue());

        p.add(checkboxes, BorderLayout.WEST);
        p.add(right, BorderLayout.CENTER);
        return p;
    }

    @SuppressWarnings("serial")
    private JPanel createJoinColumnsPanel() {
        JPanel p = new JPanel();
        p.setLayout(new BoxLayout(p, BoxLayout.Y_AXIS));
        p.setBorder(BorderFactory.createTitledBorder("Join columns"));

        m_columnPairs = new ColumnPairsSelectionPanel(true) {
            @Override
            protected void initComboBox(final DataTableSpec spec,
                @SuppressWarnings("rawtypes") final JComboBox comboBox, final String selected) {
                super.initComboBox(spec, comboBox, selected);
                // the first entry is the row id, set as default.
                if (selected == null) {
                    comboBox.setSelectedIndex(0);
                }
            }
        };

        // two elements in one row
        JPanel compositionMode = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        compositionMode.add(new JLabel("  Match "));
        compositionMode.add(m_compositionMode.getButton(CompositionModeButtonGroup.MATCH_ALL.name()));
        compositionMode.add(m_compositionMode.getButton(CompositionModeButtonGroup.MATCH_ANY.name()));
        p.add(compositionMode);

        JScrollPane scrollPane = new JScrollPane(m_columnPairs);
        m_columnPairs.setBackground(Color.white);

        Component header = m_columnPairs.getHeaderView();
        header.setPreferredSize(new Dimension(300, 20));
        scrollPane.setColumnHeaderView(header);
        scrollPane.setPreferredSize(new Dimension(300, 200));
        scrollPane.setMinimumSize(new Dimension(300, 100));

        p.add(scrollPane);

        // three elements in one row
        JPanel dataCellComparisonMode = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        dataCellComparisonMode.add(new JLabel("  Compare values in join columns by "));
        dataCellComparisonMode.add(m_dataCellComparisonMode.getButton(DataCellComparisonModeButtonGroup.STRICT.name()));
        dataCellComparisonMode.add(m_dataCellComparisonMode.getButton(DataCellComparisonModeButtonGroup.STRING.name()));
        dataCellComparisonMode
            .add(m_dataCellComparisonMode.getButton(DataCellComparisonModeButtonGroup.NUMERIC.name()));
        p.add(dataCellComparisonMode);

        return p;

    }

    private JPanel createOutputPanel() {
        JPanel p = new JPanel(new GridLayout(3, 1));
        p.setBorder(BorderFactory.createTitledBorder("Output options"));

        p.add(m_outputUnmatchedRowsToSeparatePorts.getComponentPanel().getComponent(0));
        p.add(m_mergeJoinColumns.getComponentPanel().getComponent(0));
        p.add(m_hilitingEnabled.getComponentPanel().getComponent(0));

        return p;
    }

    private JPanel createRowKeysPanel() {
        JPanel p = new JPanel(new GridLayout(2, 1));
        p.setBorder(BorderFactory.createTitledBorder("Row Keys"));

        // two elements in one row: button for concat and field for separator
        JPanel concatRowKeys = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        concatRowKeys.add(m_rowKeyFactory.getButton(RowKeyFactoryButtonGroup.CONCATENATE.name()));
        Component rowKeySeparatorComponent = m_rowKeySeparator.getComponentPanel().getComponent(1);
        concatRowKeys.add(rowKeySeparatorComponent);
        p.add(concatRowKeys);

        p.add(m_rowKeyFactory.getButton(RowKeyFactoryButtonGroup.SEQUENTIAL.name()));

        return p;
    }

    private JComponent createColumnSelectionTab() {
        JPanel p = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTHWEST;
        c.gridx = 0;
        c.gridy = 0;

        c.weightx = 1;
        c.gridwidth = 1;
        m_leftFilterPanel.setBorder(BorderFactory.createTitledBorder("Top Input (left table)"));
        p.add(m_leftFilterPanel, c);
        c.gridy++;
        m_rightFilterPanel.setBorder(BorderFactory.createTitledBorder("Bottom Input (right table)"));
        p.add(m_rightFilterPanel, c);
        c.gridy++;
        p.add(createDuplicateColumnNamesPanel(), c);
        return new JScrollPane(p);
    }

    private JPanel createDuplicateColumnNamesPanel() {
        JPanel p = new JPanel(new GridLayout(3, 1));
        p.setBorder(BorderFactory.createTitledBorder("Duplicate column names"));

        p.add(m_columnDisambiguation.getButton(ColumnNameDisambiguationButtonGroup.DO_NOT_EXECUTE.name()));

        // two elements in one row: button and suffix input field
        JPanel suffix = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        suffix.add(m_columnDisambiguation.getButton(ColumnNameDisambiguationButtonGroup.APPEND_SUFFIX.name()));
        Component suffixComponent = m_columnDisambiguationSuffix.getComponentPanel().getComponent(1);
        suffix.add(suffixComponent);
        p.add(suffix);

        return p;
    }

    private JComponent createPerformanceTab() {
        JPanel p = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();

        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTHWEST;
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;
        c.gridwidth = 1;
        c.weighty = 0;
        p.add(createOutputOrderPanel(), c);

        c.gridy++;
        JPanel misc = new JPanel(new GridLayout(2, 1));
        misc.setBorder(BorderFactory.createTitledBorder("Miscellaneous"));
        JPanel maxFiles = new JPanel(new FlowLayout(FlowLayout.LEFT));
        maxFiles.add(m_maxOpenFiles.getComponentPanel().getComponent(0));
        maxFiles.add(m_maxOpenFiles.getComponentPanel().getComponent(0)); // yes, this delivers the second component
        misc.add(maxFiles);
        p.add(misc, c);

        c.gridy++;
        c.weightx = 100;
        c.weighty = 100;
        c.fill = GridBagConstraints.BOTH;
        p.add(new JPanel(), c);

        return p;
    }

    private JPanel createOutputOrderPanel() {
        JPanel p = new JPanel(new GridLayout(3, 1));
        p.setBorder(BorderFactory.createTitledBorder("Output order"));
        p.add(m_outputRowOrder.getButton("ARBITRARY"));
        p.add(m_outputRowOrder.getButton("LEFT_RIGHT"));
        return p;
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec[] specs)
        throws NotConfigurableException {

        try {
            m_settings.loadSettingsInDialog(settings, specs);
        } catch (InvalidSettingsException e) { // NOSONAR
            throw new NotConfigurableException(e.getMessage());
        }

        // join columns
        String[] leftSelected = joinClauseToColumnPairs(m_settings.getLeftJoinColumns());
        String[] rightSelected = joinClauseToColumnPairs(m_settings.getRightJoinColumns());
        m_columnPairs.updateData(specs, leftSelected, rightSelected);

        // include column selection
        m_leftFilterPanel.loadConfiguration(m_settings.getLeftColumnSelectionConfig(), specs[0]);
        m_rightFilterPanel.loadConfiguration(m_settings.getRightColumnSelectionConfig(), specs[1]);

    }

    /**
     * Transform the {@link JoinColumn}s used in the settings and the joiner API back to {@link String}s, as understood
     * by the {@link ColumnPairsSelectionPanel}.
     *
     * @param joinColumns as stored in {@link Joiner3Settings#getLeftJoinColumns()} or
     *            {@link Joiner3Settings#getRightJoinColumns()}
     * @return string representation as understood by
     *         {@link ColumnPairsSelectionPanel#updateData(DataTableSpec[], String[], String[])}
     * @see #columnPairsToJoinClauses(Object[])
     */
    private String[] joinClauseToColumnPairs(final JoinColumn[] joinColumns) {
        return Arrays.stream(joinColumns)
            .map(jc -> jc.isColumn() ? jc.toColumnName() : m_columnPairs.getRowKeyIdentifier()).toArray(String[]::new);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {

        // save independent configurations for column filter dialog components
        m_leftFilterPanel.saveConfiguration(m_settings.getLeftColumnSelectionConfig());
        m_rightFilterPanel.saveConfiguration(m_settings.getRightColumnSelectionConfig());

        // join columns
        m_settings.setLeftJoinColumns(columnPairsToJoinClauses(m_columnPairs.getLeftSelectedItems()));
        m_settings.setRightJoinColumns(columnPairsToJoinClauses(m_columnPairs.getRightSelectedItems()));

        // output
        m_settings.saveSettingsTo(settings);
    }

    /**
     * Converts the left/right hand sides entered in the {@link ColumnPairsSelectionPanel} to {@link JoinColumn} objects
     *
     * @param clauses as retrieved by {@link ColumnPairsSelectionPanel#getLeftSelectedItems()} or
     *            {@link ColumnPairsSelectionPanel#getRightSelectedItems()}
     * @throws InvalidSettingsException
     * @see {@link #joinClauseToColumnPairs(JoinColumn[])}
     */
    private JoinColumn[] columnPairsToJoinClauses(final Object[] clauses) throws InvalidSettingsException {
        JoinColumn[] result = new JoinColumn[clauses.length];
        for (int i = 0; i < clauses.length; i++) {
            if (clauses[i] == null) {
                throw new InvalidSettingsException(
                    "There are invalid  joining columns (highlighted with a red border).");
            }
            if (clauses[i] == m_columnPairs.getRowKeyIdentifier()) {
                result[i] = new JoinColumn(SpecialJoinColumn.ROW_KEY);
            } else {
                result[i] = new JoinColumn(((DataColumnSpec)clauses[i]).getName());
            }
        }
        return result;
    }

}
