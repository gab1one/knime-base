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
 */
package org.knime.time.node.convert.oldtonew;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * The factory class of the node which converts old to new date&time types.
 *
 * @author Simon Schmid, KNIME.com, Konstanz, Germany
 */
public final class OldToNewTimeNodeFactory extends WebUINodeFactory<OldToNewTimeNodeModel> {

    private static final WebUINodeConfiguration CONFIG = WebUINodeConfiguration.builder()//
        .name("Legacy Date&Time to Date&Time") //
        .icon("old_to_new.png") //
        .shortDescription("Converts old Date&amp;Time to new.") //
        .fullDescription("""
                This node converts columns from the old Date&amp;Time data type to
                the new types. It is only useful if
                you have to use nodes that have
                not been adapted to the new Date&amp;Time type yet.
                """)//
        .modelSettingsClass(OldToNewTimeNodeParameters.class) //
        .addInputPort("Table to be converted", BufferedDataTable.TYPE, "Input table.") //
        .addOutputPort("Extracted converted", BufferedDataTable.TYPE, "Output table "
            + "containing the converted columns.") //
        .nodeType(NodeType.Manipulator) //
        .build();

    /**
    * Default constructor for the node factory.
    */
    public OldToNewTimeNodeFactory() {
        super(CONFIG);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OldToNewTimeNodeModel createNodeModel() {
        return new OldToNewTimeNodeModel();
    }

}
