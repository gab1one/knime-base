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
 *   Sep 9, 2020 (Mark Ortmann, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.filehandling.utility.nodes.metainfo;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.MissingCell;
import org.knime.core.data.container.AbstractCellFactory;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.def.BooleanCell.BooleanCellFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.util.UniqueNameGenerator;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.connections.location.FSPathProvider;
import org.knime.filehandling.core.connections.location.MultiFSPathProviderFactory;
import org.knime.filehandling.core.connections.meta.FSDescriptorRegistry;
import org.knime.filehandling.core.connections.meta.FSType;
import org.knime.filehandling.core.data.location.FSLocationValue;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;
import org.knime.filehandling.core.util.FSLocationColumnUtils;
import org.knime.filehandling.utility.nodes.metainfo.attributes.BasicKNIMEFileAttributesConverter;
import org.knime.filehandling.utility.nodes.metainfo.attributes.KNIMEFileAttributes;
import org.knime.filehandling.utility.nodes.metainfo.attributes.KNIMEFileAttributesConverter;
import org.knime.filehandling.utility.nodes.metainfo.attributes.KNIMEFileAttributesWithPermissions;
import org.knime.filehandling.utility.nodes.metainfo.attributes.KNIMEFileAttributesWithoutPermissions;
import org.knime.filehandling.utility.nodes.metainfo.attributes.PermissionsKNIMEFileAttributesConverter;
import org.knime.filehandling.utility.nodes.metainfo.attributes.PosixKNIMEFileAttributesConverter;

/**
 * The node model allowing to extract meta information about files and folders.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 */
final class FileFolderMetaInfoNodeModel extends NodeModel {

    private static final String CFG_APPEND_PERMISSIONS = "append_permissions";

    private static final String CFG_APPEND_POSIX_ATTRS = "append_posix_attrs";

    private final int m_inputFsConnectionIdx;

    private final int m_inputTableIdx;

    private final SettingsModelString m_selectedColumn;

    private final SettingsModelBoolean m_failIfPathNotExists;

    private final SettingsModelBoolean m_calculateOverallFolderSize;

    private final SettingsModelBoolean m_appendPermissions;

    private final SettingsModelBoolean m_appendPosixAttrs;

    static SettingsModelString createColumnSettingsModel() {
        return new SettingsModelString("column", null);
    }

    static SettingsModelBoolean createFailIfPathNotExistsSettingsModel() {
        return new SettingsModelBoolean("file_if_path_not_exists", true);
    }

    static SettingsModelBoolean createCalculateOverallFolderSizeSettingsModel() {
        return new SettingsModelBoolean("calculate_overall_folder_size", false);
    }

    static SettingsModelBoolean createAppendPermissionsSettingsModel() {
        return new SettingsModelBoolean(CFG_APPEND_PERMISSIONS, false);
    }

    static SettingsModelBoolean createAppendPosixAttrsSettingsModel() {
        return new SettingsModelBoolean(CFG_APPEND_POSIX_ATTRS, false);
    }

    FileFolderMetaInfoNodeModel(final PortsConfiguration config) {
        super(config.getInputPorts(), config.getOutputPorts());
        final Map<String, int[]> inputPortLocation = config.getInputPortLocation();
        m_inputFsConnectionIdx =
            Optional.ofNullable(inputPortLocation.get(FileFolderMetaInfoNodeFactory.CONNECTION_INPUT_PORT_GRP_NAME))//
                .map(a -> a[0])//
                .orElseGet(() -> -1);
        m_inputTableIdx = inputPortLocation.get(FileFolderMetaInfoNodeFactory.DATA_TABLE_INPUT_PORT_GRP_NAME)[0];
        m_selectedColumn = createColumnSettingsModel();
        m_failIfPathNotExists = createFailIfPathNotExistsSettingsModel();
        m_calculateOverallFolderSize = createCalculateOverallFolderSizeSettingsModel();
        m_appendPermissions = createAppendPermissionsSettingsModel();
        m_appendPosixAttrs = createAppendPosixAttrsSettingsModel();
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final DataTableSpec inputTableSpec = ((DataTableSpec)inSpecs[m_inputTableIdx]);

        if (m_selectedColumn.getStringValue() == null) {
            autoGuess(inSpecs);
            setWarningMessage(String.format("Auto-guessed column containing file/folder paths '%s'",
                m_selectedColumn.getStringValue()));
        }
        validateSettings(inSpecs);

        final int pathColIdx = inputTableSpec.findColumnIndex(m_selectedColumn.getStringValue());
        final KNIMEFileAttributesConverter[] fileAttrConverters = getFileAttributesConverter();
        try (final FileAttributesFactory fac = new FileAttributesFactory(fileAttrConverters,
            createNewColumns(inputTableSpec, fileAttrConverters), pathColIdx, getFSConnection(inSpecs), false)) {
            return new PortObjectSpec[]{createColumnRearranger(inputTableSpec, fac).createSpec()};
        }
    }

    private KNIMEFileAttributesConverter[] getFileAttributesConverter() {
        List<KNIMEFileAttributesConverter> converters = new ArrayList<>();
        converters.addAll(Arrays.asList(BasicKNIMEFileAttributesConverter.values()));

        if (m_appendPermissions.getBooleanValue()) {
            converters.addAll(Arrays.asList(PermissionsKNIMEFileAttributesConverter.values()));
        }

        if (m_appendPosixAttrs.getBooleanValue()) {
            converters.addAll(Arrays.asList(PosixKNIMEFileAttributesConverter.values()));
        }

        return converters.toArray(KNIMEFileAttributesConverter[]::new);
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final PortObjectSpec[] inSpecs = Arrays.stream(inObjects)//
            .map(PortObject::getSpec)//
            .toArray(PortObjectSpec[]::new);

        final DataTableSpec inputTableSpec = (DataTableSpec)inSpecs[m_inputTableIdx];
        final int pathColIdx = inputTableSpec.findColumnIndex(m_selectedColumn.getStringValue());
        final KNIMEFileAttributesConverter[] fileAttrConverters = getFileAttributesConverter();

        try (final FileAttributesFactory fac = new FileAttributesFactory(fileAttrConverters,
            createNewColumns(inputTableSpec, fileAttrConverters), pathColIdx, getFSConnection(inSpecs), true)) {
            return new PortObject[]{exec.createColumnRearrangeTable((BufferedDataTable)inObjects[m_inputTableIdx],
                createColumnRearranger(inputTableSpec, fac), exec)};
        }
    }

    private void autoGuess(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final DataTableSpec inputTableSpec = (DataTableSpec)inSpecs[m_inputTableIdx];
        m_selectedColumn.setStringValue(inputTableSpec.stream()//
            .filter(dcs -> dcs.getType().isCompatible(FSLocationValue.class))//
            .map(DataColumnSpec::getName)//
            .findFirst()//
            .orElseThrow(() -> new InvalidSettingsException("No applicable column available"))//
        );
    }

    private void validateSettings(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final DataTableSpec inSpec = (DataTableSpec)inSpecs[m_inputTableIdx];
        final String pathColName = m_selectedColumn.getStringValue();
        final int colIndex = inSpec.findColumnIndex(pathColName);

        // check column existence
        CheckUtils.checkSetting(colIndex >= 0, "The selected column '%s' is not part of the input", pathColName);

        // validate the selected column
        final DataColumnSpec pathColSpec = inSpec.getColumnSpec(colIndex);
        final Optional<String> warningMsg = FSLocationColumnUtils.validateFSLocationColumn(pathColSpec,
            m_inputFsConnectionIdx >= 0 ? (FileSystemPortObjectSpec)inSpecs[m_inputFsConnectionIdx] : null);
        warningMsg.ifPresent(this::setWarningMessage);
    }

    private static ColumnRearranger createColumnRearranger(final DataTableSpec inputTableSpec,
        final FileAttributesFactory fac) {
        final ColumnRearranger colRearranger = new ColumnRearranger(inputTableSpec);
        colRearranger.append(fac);
        return colRearranger;
    }

    private FSConnection getFSConnection(final PortObjectSpec[] inSpecs) {
        return m_inputFsConnectionIdx < 0 //
            ? null //
            : ((FileSystemPortObjectSpec)inSpecs[0]).getFileSystemConnection().orElse(null);
    }

    private static DataColumnSpec[] createNewColumns(final DataTableSpec inputTableSpec,
        final KNIMEFileAttributesConverter[] fileAttrConverters) {
        final UniqueNameGenerator uniqueNameGen = new UniqueNameGenerator(inputTableSpec);
        return Arrays.stream(fileAttrConverters)//
            .map(attr -> uniqueNameGen.newColumn(attr.getName(), attr.getType()))//
            .toArray(DataColumnSpec[]::new);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_selectedColumn.saveSettingsTo(settings);
        m_failIfPathNotExists.saveSettingsTo(settings);
        m_calculateOverallFolderSize.saveSettingsTo(settings);
        m_appendPermissions.saveSettingsTo(settings);
        m_appendPosixAttrs.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_selectedColumn.validateSettings(settings);
        m_failIfPathNotExists.validateSettings(settings);
        m_calculateOverallFolderSize.validateSettings(settings);
        // load backwards compatible see AP-15123
        if (settings.containsKey(CFG_APPEND_PERMISSIONS)) {
            m_appendPermissions.validateSettings(settings);
        }
        if (settings.containsKey(CFG_APPEND_POSIX_ATTRS)) {
            m_appendPosixAttrs.validateSettings(settings);
        }
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_selectedColumn.loadSettingsFrom(settings);
        m_failIfPathNotExists.loadSettingsFrom(settings);
        m_calculateOverallFolderSize.loadSettingsFrom(settings);
        // load backwards compatible see AP-15123
        if (settings.containsKey(CFG_APPEND_PERMISSIONS)) {
            m_appendPermissions.loadSettingsFrom(settings);
        }
        if (settings.containsKey(CFG_APPEND_POSIX_ATTRS)) {
            m_appendPosixAttrs.loadSettingsFrom(settings);
        }
    }

    @Override
    protected void reset() {
        // nothing to do

    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) {
        // nothing to do
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) {
        // nothing to do
    }

    private final class FileAttributesFactory extends AbstractCellFactory implements Closeable {

        private final KNIMEFileAttributesConverter[] m_fileAttrConverters;

        private final int m_existsIdx;

        private final int m_colIdx;

        private final MultiFSPathProviderFactory m_multiFSPathProviderCellFactory;

        @SuppressWarnings("resource") // the MultiFSPathProviderFactory will be closed in #close
        FileAttributesFactory(final KNIMEFileAttributesConverter[] fileAttrConverters, final DataColumnSpec[] colSpecs,
            final int pathColIdx, final FSConnection fsConnection, final boolean initFactory) {
            super(colSpecs);
            m_fileAttrConverters = fileAttrConverters;
            m_existsIdx = IntStream.range(0, m_fileAttrConverters.length)//
                .filter(i -> BasicKNIMEFileAttributesConverter.EXISTS == m_fileAttrConverters[i])//
                .findFirst()//
                .orElse(-1);
            m_colIdx = pathColIdx;
            m_multiFSPathProviderCellFactory = initFactory ? new MultiFSPathProviderFactory(fsConnection) : null;
        }

        @Override
        public DataCell[] getCells(final DataRow row) {
            final DataCell c = row.getCell(m_colIdx);
            if (c.isMissing()) {
                return createMissingCells(DataType::getMissingCell);
            } else {
                return createCells((FSLocationValue)c);
            }
        }

        private DataCell[] createCells(final FSLocationValue cell) {
            try (final FSPathProvider pathProvder = m_multiFSPathProviderCellFactory
                .getOrCreateFSPathProviderFactory(cell.getFSLocation()).create(cell.getFSLocation())) {
                final FSPath path = pathProvder.getPath();
                if (!FSFiles.exists(path) && !m_failIfPathNotExists.getBooleanValue()) {
                    final DataCell[] cells = createMissingCells(() -> new MissingCell("File/folder does not exist"));
                    if (m_existsIdx >= 0) {
                        cells[m_existsIdx] = BooleanCellFactory.create(false);
                    }
                    return cells;
                } else {
                    return createCells(path);
                }
            } catch (final NoSuchFileException e) { //NOSONAR
                throw new IllegalArgumentException(String.format("The file/folder '%s' does not exist", e.getMessage()),
                    e);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        private DataCell[] createMissingCells(final Supplier<DataCell> supplier) {
            return Stream.generate(supplier)//
                .limit(m_fileAttrConverters.length)//
                .toArray(DataCell[]::new);
        }

        private DataCell[] createCells(final FSPath path) throws IOException {
            final KNIMEFileAttributes attributes = getFileAttributes(path);
            return Arrays.stream(m_fileAttrConverters)//
                .map(a -> a.createCell(attributes))//
                .toArray(DataCell[]::new);
        }

        private KNIMEFileAttributes getFileAttributes(final FSPath path) throws IOException {
            boolean hasPosixAttrs = m_appendPosixAttrs.getBooleanValue() && hasPosixAttributes(path);
            Class<? extends BasicFileAttributes> attrsClass =
                hasPosixAttrs ? PosixFileAttributes.class : BasicFileAttributes.class;
            final BasicFileAttributes basicAttributes = Files.readAttributes(path, attrsClass);

            if (m_appendPermissions.getBooleanValue()) {
                return new KNIMEFileAttributesWithPermissions(path, m_calculateOverallFolderSize.getBooleanValue(),
                    basicAttributes, hasPosixAttrs);
            } else {
                return new KNIMEFileAttributesWithoutPermissions(path, m_calculateOverallFolderSize.getBooleanValue(),
                    basicAttributes, hasPosixAttrs);
            }
        }

        @SuppressWarnings("resource")
        private boolean hasPosixAttributes(final FSPath path) {
            FSType fsType = path.getFileSystem().getFSType();
            return FSDescriptorRegistry.getFSDescriptor(fsType)//
                .map(d -> d.getCapabilities().canGetPosixAttributes())
                .orElseThrow(() -> new IllegalStateException("File system not found: " + fsType.getName()));
        }

        @Override
        public void close() {
            if (m_multiFSPathProviderCellFactory != null) {
                m_multiFSPathProviderCellFactory.close();
            }
        }
    }
}
