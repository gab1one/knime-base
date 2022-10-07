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
 * --------------------------------------------------------------------
 *
 * History
 *   03.07.2007 (cebron): created
 */
package org.knime.base.node.preproc.pmml.stringtonumber3;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;

import org.knime.base.node.preproc.pmml.PMMLStringConversionTranslator;
import org.knime.base.node.util.spec.TableSpecUtils;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.StringValue;
import org.knime.core.data.container.CellFactory;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpec;
import org.knime.core.node.port.pmml.PMMLPortObjectSpecCreator;
import org.knime.core.node.port.pmml.preproc.DerivedFieldMapper;

/**
 * The NodeModel for the String to Number Node that converts strings to numbers.
 *
 * @author cebron, University of Konstanz
 * @param <T> SettingsModel for a ColumnFilter component
 * @since 4.0
 */

public abstract class AbstractStringToNumberNodeModel<T extends SettingsModel> extends NodeModel {

    /** The filter value class. */
    private static final Class<StringValue> VALUE_CLASS = StringValue.class;

    /**
     * The possible types that the string can be converted to.
     */
    public static final DataType[] POSSIBLETYPES = new DataType[]{DoubleCell.TYPE, IntCell.TYPE}; // NOSONAR public API

    /* Node Logger of this class. */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(AbstractStringToNumberNodeModel.class);

    /**
     * Key for the included columns in the NodeSettings.
     */
    public static final String CFG_INCLUDED_COLUMNS = "include";

    /**
     * Key for the decimal separator in the NodeSettings.
     */
    public static final String CFG_DECIMALSEP = "decimal_separator";

    /**
     * Key for the thousands separator in the NodeSettings.
     */
    public static final String CFG_THOUSANDSSEP = "thousands_separator";

    /**
     * Key for the parsing type in the NodeSettings.
     */
    public static final String CFG_PARSETYPE = "parse_type";

    /**
     * The default decimal separator.
     */
    public static final String DEFAULT_DECIMAL_SEPARATOR = ".";

    /**
     * The default thousands separator.
     */
    public static final String DEFAULT_THOUSANDS_SEPARATOR = "";

    /** The included columns. */
    private final T m_inclCols;

    /*
     * The decimal separator
     */
    private String m_decimalSep = DEFAULT_DECIMAL_SEPARATOR;

    /*
     * The thousands separator
     */
    private String m_thousandsSep = DEFAULT_THOUSANDS_SEPARATOR;

    private DataType m_parseType = POSSIBLETYPES[0];

    /** if there should be an optional pmml input port. */
    private boolean m_pmmlInEnabled;

    /**
     * Constructor with one data inport, one data outport and an optional PMML inport and outport.
     *
     * @param inclCols SettingsModel for a ColumnFilter component
     */
    public AbstractStringToNumberNodeModel(final T inclCols) {
        this(true, inclCols);
    }

    /**
     * Constructor with one data inport, one data outport and an optional PMML inport and outport.
     *
     * @param pmmlInEnabled true if an optional PMML input port should be present
     * @param inclCols SettingsModel for a ColumnFilter component
     * @since 3.0
     */
    public AbstractStringToNumberNodeModel(final boolean pmmlInEnabled, final T inclCols) {
        super(pmmlInEnabled ? new PortType[]{BufferedDataTable.TYPE, PMMLPortObject.TYPE_OPTIONAL}
            : new PortType[]{BufferedDataTable.TYPE}, new PortType[]{BufferedDataTable.TYPE, PMMLPortObject.TYPE});
        m_inclCols = inclCols;
        m_pmmlInEnabled = pmmlInEnabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        // find indices to work on
        DataTableSpec dts = (DataTableSpec)inSpecs[0];
        int[] indices = findColumnIndices(dts);
        ConverterFactory converterFac = new ConverterFactory(indices, dts, m_parseType);
        ColumnRearranger colre = new ColumnRearranger(dts);
        colre.replace(converterFac, indices);
        DataTableSpec newspec = colre.createSpec();

        // create the PMML spec based on the optional incoming PMML spec
        PMMLPortObjectSpec pmmlSpec = m_pmmlInEnabled ? (PMMLPortObjectSpec)inSpecs[1] : null;
        PMMLPortObjectSpecCreator pmmlSpecCreator = new PMMLPortObjectSpecCreator(pmmlSpec, dts);

        return new PortObjectSpec[]{newspec, pmmlSpecCreator.createSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        StringBuilder warnings = new StringBuilder();
        BufferedDataTable inData = (BufferedDataTable)inObjects[0];
        DataTableSpec inSpec = inData.getDataTableSpec();
        // find indices to work on.
        String[] inclCols = getInclCols(inSpec);
        BufferedDataTable resultTable = null;
        if (inclCols.length == 0) {
            // nothing to convert, let's return the input table.
            resultTable = inData;
            setWarningMessage("No columns selected," + " returning input DataTable.");
        } else {
            int[] indices = findColumnIndices(inSpec);
            ConverterFactory converterFac = new ConverterFactory(indices, inSpec, m_parseType);
            ColumnRearranger colre = new ColumnRearranger(inSpec);
            colre.replace(converterFac, indices);

            resultTable = exec.createColumnRearrangeTable(inData, colre, exec);
            String errorMessage = converterFac.getErrorMessage();

            if (errorMessage.length() > 0) {
                warnings.append("Problems occurred, see Console messages.\n");
            }
            if (warnings.length() > 0) {
                LOGGER.warn(errorMessage);
                setWarningMessage(warnings.toString());
            }
        }

        // the optional PMML in port (can be null)
        PMMLPortObject inPMMLPort = m_pmmlInEnabled ? (PMMLPortObject)inObjects[1] : null;
        PMMLStringConversionTranslator trans = new PMMLStringConversionTranslator(
            Arrays.asList(getInclCols(inSpec)), m_parseType, new DerivedFieldMapper(inPMMLPort));

        PMMLPortObjectSpecCreator creator = new PMMLPortObjectSpecCreator(inPMMLPort, inSpec);
        PMMLPortObject outPMMLPort = new PMMLPortObject(creator.createSpec(), inPMMLPort, inSpec);
        outPMMLPort.addGlobalTransformations(trans.exportToTransDict());

        return new PortObject[]{resultTable, outPMMLPort};
    }

    private int[] findColumnIndices(final DataTableSpec spec) throws InvalidSettingsException {
        final String[] inclCols = getInclCols(spec);
        final StringBuilder warnings = new StringBuilder();
        if (inclCols.length == 0) {
            warnings.append("No columns selected");
        }
        final int[] indices;
        if (isKeepAllSelected()) {
            indices = TableSpecUtils.findAllCompatibleColumns(spec, VALUE_CLASS);
        } else {
            indices = TableSpecUtils.findCompatibleColumns(spec, inclCols, VALUE_CLASS, warnings::append);
        }
        if (warnings.length() > 0) {
            setWarningMessage(warnings.toString());
        }
        return indices;
    }

    /**
     * Returns all present includes from a DataTableSpec.
     *
     * @param inSpec the current DataTableSpec
     * @return a String array with the included columns
     * @since 4.5
     */
    protected abstract String[] getInclCols(final DataTableSpec inSpec);

    /**
     * @return returns true if the keep all selected checkbox is checked, false if it is not checked or not present
     */
    protected abstract boolean isKeepAllSelected();

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // empty
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_inclCols.loadSettingsFrom(settings);
        m_decimalSep = settings.getString(CFG_DECIMALSEP, DEFAULT_DECIMAL_SEPARATOR);
        m_thousandsSep = settings.getString(CFG_THOUSANDSSEP, DEFAULT_THOUSANDS_SEPARATOR);
        m_parseType = settings.getDataType(CFG_PARSETYPE, POSSIBLETYPES[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_inclCols.saveSettingsTo(settings);
        settings.addString(CFG_DECIMALSEP, m_decimalSep);
        settings.addString(CFG_THOUSANDSSEP, m_thousandsSep);
        settings.addDataType(CFG_PARSETYPE, m_parseType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_inclCols.validateSettings(settings);
        String decimalsep = settings.getString(CFG_DECIMALSEP, DEFAULT_DECIMAL_SEPARATOR);
        String thousandssep = settings.getString(CFG_THOUSANDSSEP, DEFAULT_THOUSANDS_SEPARATOR);
        if (decimalsep == null || thousandssep == null) {
            throw new InvalidSettingsException("Separators must not be null");
        }
        if (decimalsep.length() > 1 || thousandssep.length() > 1) {
            throw new InvalidSettingsException("Illegal separator length, expected a single character");
        }

        if (decimalsep.equals(thousandssep)) {
            throw new InvalidSettingsException("Decimal and thousands separator must not be the same.");
        }
        DataType myType = settings.getDataType(CFG_PARSETYPE, POSSIBLETYPES[0]);
        boolean found = false;
        for (DataType type : POSSIBLETYPES) {
            if (type.equals(myType)) {
                found = true;
            }
        }
        if (!found) {
            throw new InvalidSettingsException("Illegal parse type: " + myType);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // empty.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // empty.
    }

    /**
     * @return the included columns
     */
    protected T getInclCols() {
        return m_inclCols;
    }

    /**
     * The CellFactory to produce the new converted cells.
     *
     * @author cebron, University of Konstanz
     */
    private class ConverterFactory implements CellFactory {

        /*
         * Column indices to use.
         */
        private final int[] m_colindices;

        /*
         * Original DataTableSpec.
         */
        private final DataTableSpec m_spec;

        /*
         * Error messages.
         */
        private String m_error;

        /** Number of parsing errors. */
        private int m_parseErrorCount;

        private final DataType m_type;

        /**
         *
         * @param colindices the column indices to use.
         * @param spec the original DataTableSpec.
         * @param type the {@link DataType} to convert to.
         */
        ConverterFactory(final int[] colindices, final DataTableSpec spec, final DataType type) {
            m_colindices = colindices;
            m_spec = spec;
            m_type = type;
            m_parseErrorCount = 0;
        }

        @Override
        public Optional<int[]> getRequiredColumns() {
            return Optional.of(m_colindices);
        }

        @Override
        public boolean hasState() {
            return false;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataCell[] getCells(final DataRow row) {
            DataCell[] newcells = new DataCell[m_colindices.length];
            for (int i = 0; i < newcells.length; i++) {
                DataCell dc = row.getCell(m_colindices[i]);
                // should be a DoubleCell, otherwise copy original cell.
                if (!dc.isMissing()) {
                    final String s = ((StringValue)dc).getStringValue();
                    if (s.trim().length() == 0) {
                        newcells[i] = DataType.getMissingCell();
                        continue;
                    }
                    try {
                        String corrected = s;
                        if (m_thousandsSep != null && m_thousandsSep.length() > 0) {
                            // remove thousands separator
                            corrected = s.replaceAll(Pattern.quote(m_thousandsSep), "");
                        }
                        if (!".".equals(m_decimalSep)) {
                            if (corrected.contains(".")) {
                                throw new NumberFormatException("Invalid floating point number");
                            }
                            if (m_decimalSep != null && m_decimalSep.length() > 0) {
                                // replace custom separator with standard
                                corrected = corrected.replaceAll(Pattern.quote(m_decimalSep), ".");
                            }
                        }

                        if (m_type.equals(DoubleCell.TYPE)) {
                            double parsedDouble = Double.parseDouble(corrected);
                            newcells[i] = new DoubleCell(parsedDouble);
                        } else if (m_type.equals(IntCell.TYPE)) {
                            int parsedInteger = Integer.parseInt(corrected);
                            newcells[i] = new IntCell(parsedInteger);
                        } else {
                            m_error = "No valid parse type.";
                        }
                    } catch (NumberFormatException e) {
                        if (m_parseErrorCount == 0) {
                            m_error = "'" + s + "' (RowKey: " + row.getKey().toString() + ", Position: "
                                + m_colindices[i] + ")";
                            LOGGER.debug(e.getMessage());
                        }
                        m_parseErrorCount++;
                        newcells[i] = DataType.getMissingCell();
                    }
                } else {
                    newcells[i] = DataType.getMissingCell();
                }
            }
            return newcells;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataColumnSpec[] getColumnSpecs() {
            DataColumnSpec[] newcolspecs = new DataColumnSpec[m_colindices.length];
            for (int i = 0; i < newcolspecs.length; i++) {
                DataColumnSpec colspec = m_spec.getColumnSpec(m_colindices[i]);
                DataColumnSpecCreator colspeccreator = null;
                if (m_type.equals(DoubleCell.TYPE)) {
                    // change DataType to DoubleCell
                    colspeccreator = new DataColumnSpecCreator(colspec.getName(), DoubleCell.TYPE);
                } else if (m_type.equals(IntCell.TYPE)) {
                    // change DataType to IntCell
                    colspeccreator = new DataColumnSpecCreator(colspec.getName(), IntCell.TYPE);
                } else {
                    colspeccreator =
                        new DataColumnSpecCreator("Invalid parse mode", DataType.getMissingCell().getType());
                }
                newcolspecs[i] = colspeccreator.createSpec();
            }
            return newcolspecs;
        }

        /**
         * {@inheritDoc}
         *
         * @deprecated
         */
        @Deprecated
        @Override
        public void setProgress(final int curRowNr, final int rowCount, final RowKey lastKey,
            final ExecutionMonitor exec) {
            exec.setProgress((double)curRowNr / (double)rowCount, "Converting");
        }

        /**
         * Error messages that occur during execution , i.e. NumberFormatException.
         *
         * @return error message
         */
        public String getErrorMessage() {
            switch (m_parseErrorCount) {
                case 0:
                    return "";
                case 1:
                    return "Could not parse cell with value " + m_error;
                default:
                    return "Values in " + m_parseErrorCount + " cells could not be parsed, first error: " + m_error;
            }
        }

    } // end ConverterFactory
}
