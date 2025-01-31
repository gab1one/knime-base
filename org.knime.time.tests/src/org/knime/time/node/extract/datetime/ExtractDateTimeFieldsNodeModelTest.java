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
 *   May 10, 2021 (Mark Ortmann): created
 */
package org.knime.time.node.extract.datetime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.knime.InputTableNode;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeSettings;
import org.knime.testing.util.TableTestUtil;
import org.knime.testing.util.WorkflowManagerUtil;

/**
 * Test class that ensures that with Java-11 all Locales without a region/country as being mapped to a proper value.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
final class ExtractDateTimeFieldsNodeModelTest {

    private static final Set<String> J_8_REGION_FREE_LOCALES =
        Arrays
            .stream(new String[]{"bg", "it", "ko", "uk", "lv", "pt", "sk", "ga", "et", "sv", "cs", "el", "hu", "id",
                "be", "es", "tr", "hr", "lt", "sq", "fr", "ja", "is", "de", "en", "ca", "sl", "fi", "mk", "sr-Latn",
                "th", "ar", "ru", "ms", "hi", "nl", "vi", "sr", "mt", "da", "ro", "no", "pl", "he", "zh"})
            .collect(Collectors.toSet());

    private static Locale getLocale(final String languageTag) throws InvalidSettingsException {
        return LocaleProvider.JAVA_8.stringToLocale(languageTag);
    }

    private static final String INPUT_COLUMN = "test_input";

    private static final String NODE_NAME = "ExtractDateTimeFieldNode";

    private static final Class<? extends DefaultNodeSettings> SETTINGS_CLASS = ExtractDateTimeFieldsSettings.class;

    /**
     * Tests that the jvm does not use COMPAT, i.e., the test is run with Java 11 and the locales.providers have not
     * been adjusted via vm-paramters.
     */
    public void ensure_compat_not_set() {
        assertFalse(ExtractDateTimeFieldsNodeModel.USES_COMPAT);
    }

    /**
     * Tests that all Java-8 region free locales, but und (undefined), have a mapping.
     */
    @Test
    public void test_all_j_8_region_free_locales_have_been_mapped() {
        assertEquals("Not all region/country free locales have been mapped", J_8_REGION_FREE_LOCALES.size(),
            ExtractDateTimeFieldsNodeModel.LOCALE_MAPPING.size());
        for (final String key : J_8_REGION_FREE_LOCALES) {
            assertTrue("Not all java 8 region free locales have been mapped",
                ExtractDateTimeFieldsNodeModel.LOCALE_MAPPING.containsKey(key));
        }
    }

    /**
     * Tests that the mapping only stores keys without a region.
     *
     * @throws InvalidSettingsException - can not happen with {@link LocaleProvider#JAVA_8}
     */
    @SuppressWarnings("javadoc")
    @Test
    public void test_keys_are_valid() throws InvalidSettingsException {
        for (final String localeTag : ExtractDateTimeFieldsNodeModel.LOCALE_MAPPING.keySet()) {
            assertTrue(getLocale(localeTag).getCountry().isEmpty());
        }
    }

    /**
     * Test that all mapped values have a region.
     *
     * @throws InvalidSettingsException - can not happen with {@link LocaleProvider#JAVA_8}
     */
    @SuppressWarnings("javadoc")
    @Test
    public void test_values_are_valid() throws InvalidSettingsException {
        for (final String localeTag : ExtractDateTimeFieldsNodeModel.LOCALE_MAPPING.values()) {
            assertFalse(getLocale(localeTag).getCountry().isEmpty());
        }
    }

    /**
     * Tests that no key is mapped to the same value.
     */
    @Test
    public void ensure_values_are_unique() {
        assertEquals(
            "Not all values are unique, i.e., different locales without region map to the same locale with region",
            ExtractDateTimeFieldsNodeModel.LOCALE_MAPPING.size(),
            new HashSet<String>(ExtractDateTimeFieldsNodeModel.LOCALE_MAPPING.values()).size());
    }

    /**
     * Tests that the mapping function works as expected.
     *
     * @throws InvalidSettingsException - can not happen with {@link LocaleProvider#JAVA_8}
     */
    @SuppressWarnings("javadoc")
    @Test
    public void test_mapping() throws InvalidSettingsException {
        for (final String l : J_8_REGION_FREE_LOCALES) {
            assertNotEquals(getLocale(l), ExtractDateTimeFieldsNodeModel.getLocale(l, true));
            assertEquals(getLocale(l), ExtractDateTimeFieldsNodeModel.getLocale(l, false));
        }
    }

    /**
     * Tests that missing value gives missing output.
     */
    @Test
    public void test_that_missing_input_gives_missing_output() throws InvalidSettingsException, IOException {
        var settings = new ExtractDateTimeFieldsSettings();
        settings.m_selectedColumn = INPUT_COLUMN;

        var testSetup = setupAndExecuteWorkflow(settings, DataType.getMissingCell());

        assertTrue(testSetup.success);
        assertTrue(testSetup.firstCell.isMissing());
    }

    @Test
    public void test_that_empty_input_gives_no_error() throws InvalidSettingsException, IOException {
        var settings = new ExtractDateTimeFieldsSettings();
        settings.m_selectedColumn = INPUT_COLUMN;

        var testSetup = setupAndExecuteWorkflow(settings, null);

        assertTrue(testSetup.success);
        assertTrue(testSetup.firstCell == null);
        assertTrue(testSetup.outputTable.size() == 0);
    }

    record TestSetup(BufferedDataTable outputTable, DataCell firstCell, boolean success) {
    }

    static TestSetup setupAndExecuteWorkflow(final ExtractDateTimeFieldsSettings settings, final DataCell cellToAdd)
        throws InvalidSettingsException, IOException {
        var workflowManager = WorkflowManagerUtil.createEmptyWorkflow();

        var node = WorkflowManagerUtil.createAndAddNode(workflowManager, new ExtractDateTimeFieldsNodeFactory2());

        // set the settings
        final var nodeSettings = new NodeSettings(NODE_NAME);
        workflowManager.saveNodeSettings(node.getID(), nodeSettings);
        var modelSettings = nodeSettings.addNodeSettings("model");
        DefaultNodeSettings.saveSettings(SETTINGS_CLASS, settings, modelSettings);
        workflowManager.loadNodeSettings(node.getID(), nodeSettings);

        // populate the input table
        var inputTableSpecBuilder = new TableTestUtil.SpecBuilder();
        if (cellToAdd != null) {
            inputTableSpecBuilder = inputTableSpecBuilder.addColumn(INPUT_COLUMN, cellToAdd.getType());
        } else {
            inputTableSpecBuilder = inputTableSpecBuilder.addColumn(INPUT_COLUMN, LocalDateTimeCellFactory.TYPE);
        }
        var inputTableSpec = inputTableSpecBuilder.build();
        var inputTableBuilder = new TableTestUtil.TableBuilder(inputTableSpec);
        if (cellToAdd != null) {
            inputTableBuilder = inputTableBuilder.addRow(cellToAdd);
        }
        var inputTable = inputTableBuilder.build();
        var tableSupplierNode =
            WorkflowManagerUtil.createAndAddNode(workflowManager, new InputTableNode.InputDataNodeFactory(inputTable));

        // link the nodes
        workflowManager.addConnection(tableSupplierNode.getID(), 1, node.getID(), 1);

        // execute and wait...
        var success = workflowManager.executeAllAndWaitUntilDone();

        var outputTable = (BufferedDataTable)node.getOutPort(1).getPortObject();

        if (outputTable.size() == 0) {
            return new TestSetup(outputTable, null, success);
        }
        try (var it = outputTable.iterator()) {
            return new TestSetup( //
                outputTable, //
                it.next().getCell(0), //
                success //
            );
        }

    }

}
