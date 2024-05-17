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
 * -------------------------------------------------------------------
 *
 * History 05.11.2006 (Tobias Koetter): created
 */
package org.knime.base.node.preproc.rowkey2;

import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * The node factory of the row key manipulation node. The node allows the user
 * to replace the row key with another column and/or to append a new column
 * with the values of the current row key.
 *
 * @author Tobias Koetter
 * @since 2.6
 */
@SuppressWarnings("restriction")
public class RowKeyNodeFactory2 extends WebUINodeFactory<RowKeyNodeModel2> {

    private static final WebUINodeConfiguration CONFIG = WebUINodeConfiguration.builder() //
        .name("RowID") //
        .icon("rowID.png") //
        .shortDescription("Node to extract the RowID to a column and/or to replace the RowID.")
        .fullDescription("""
                This node can be used to extract the values of the current RowID of an input table to a new column.

                The node can also be used to replace the current RowID of the input table with a generated RowID of the
                format: Row0, Row1, Row2, ..., or by the values of a selected column (by converting the values to
                string). In the latter case, the node provides options to ensure uniqueness and to handle missing values
                in the selected column.

                If both extraction of the RowID and replacement of the RowID are configured, the node appends a new
                column with the values of the current RowID to the table and replaces the current RowID with the values
                of the selected column or the generated RowID.

                Note: Hiliting does not work across this node if the "Enable hiliting" option is disabled.
                """) //
        .modelSettingsClass(RowKeyNodeSettings.class) //
        .nodeType(NodeType.Manipulator) //
        .addInputTable("Input Table", "The data table whose RowID to extract and/or replace.") //
        .addOutputTable("Output Table",
            "The input table with a new column of extracted RowID values and/or with its RowID replaced.") //
        .sinceVersion(2, 6, 0) //
        .build();

    /**
     * Creates a new instance of this factory.
     */
    public RowKeyNodeFactory2() {
        super(CONFIG);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowKeyNodeModel2 createNodeModel() {
        return new RowKeyNodeModel2(CONFIG);
    }


}
