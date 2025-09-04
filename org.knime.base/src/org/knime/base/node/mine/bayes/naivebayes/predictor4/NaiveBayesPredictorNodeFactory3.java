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
 *   Nov 27, 2018 (Mark Ortmann, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.base.node.mine.bayes.naivebayes.predictor4;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.xmlbeans.XmlException;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeDialog;
import org.knime.node.impl.description.DefaultNodeDescriptionUtil;
import org.knime.node.impl.description.PortDescription;
import org.xml.sax.SAXException;

/**
 * <code>NodeFactory</code> for the "Naive Bayes Predictor" node.
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 * @noreference This class is not intended to be referenced by clients.
 */
@SuppressWarnings("restriction")
public final class NaiveBayesPredictorNodeFactory3 extends NodeFactory<NaiveBayesPredictorNodeModel3>
    implements NodeDialogFactory {
    /**
     * {@inheritDoc}
     */
    @Override
    public NaiveBayesPredictorNodeModel3 createNodeModel() {
        return new NaiveBayesPredictorNodeModel3();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<NaiveBayesPredictorNodeModel3> createNodeView(final int viewIndex,
        final NaiveBayesPredictorNodeModel3 nodeModel) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @since 5.8
     */
    @Override
    public NodeDialog createNodeDialog() {
        return new DefaultNodeDialog(SettingsType.MODEL, NaiveBayesPredictorNodeParameters.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    /**
     * Use the WebUINodeConfiguration to generate the node description (replacing the XML file).
     */
    @Override
    protected NodeDescription createNodeDescription() throws SAXException, IOException, XmlException {
        Collection<PortDescription> inPortDescriptions = List.of(//
            new PortDescription("modelId", "The naive Bayes model to use", "A previously learned naive Bayes model",
                false), //
            new PortDescription("tableId", "Input data to classify", "Input data to classify", false));
        Collection<PortDescription> outPortDescriptions = List.of(//
            new PortDescription("outId", "The classified data", //
                "The input table with one column added containing the \n"//
                    + "        classification and the probabilities depending on the options.", //
                false));

        return DefaultNodeDescriptionUtil.createNodeDescription("Naive Bayes Predictor", //
            "./naiveBayesPredictor.png", //
            inPortDescriptions, //
            outPortDescriptions, //
            """

                            Uses the PMML naive Bayes model from the naive Bayes learner to predict
                    the class membership of each row in the input data.
                    """, """
                    Predicts the class per row based on the learned model. The class
                    probability is the product of the probability per attribute and the
                    probability of the class attribute itself.
                    <p>
                        The probability for nominal values is the number of occurrences of
                        the class value with the given value divided by the number of total
                        occurrences of the class value. The probability of numerical values
                        is calculated by assuming a normal distribution per attribute.
                    </p>
                    """, //
            List.of(), // resources
            NaiveBayesPredictorNodeParameters.class, //
            List.of(), // view descriptions
            NodeType.Predictor, //
            List.of(), // keywords
            null);
    }

}
