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
 *   Mar 3, 2021 (Mark Ortmann, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.filehandling.utility.nodes.metainfo.attributes;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;

/**
 * Class wrapping {@link BasicFileAttributes} and exposing additional functionality required by all
 * {@link KNIMEFileAttributesConverter}s, except those relying on file/folder permissions.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 */
public final class KNIMEFileAttributesWithoutPermissions extends KNIMEFileAttributes {

    /**
     * Constructor allowing to calculate the overall size of the provided path if it is a folder, but not the
     * file/folder permissions. When invoking any method related to file permissions an
     * {@link UnsupportedOperationException} is thrown.
     *
     * @param p the path
     * @param calcFolderSize if {@code true} the size of overall size of the folder will be calculated
     * @param fileAttributes the path's {@link BasicFileAttributes}
     * @throws IOException - If anything went wrong while calculating the folders size
     */
    public KNIMEFileAttributesWithoutPermissions(final Path p, final boolean calcFolderSize,
        final BasicFileAttributes fileAttributes) throws IOException {
        this(p, calcFolderSize, fileAttributes, false);
    }

    /**
     * Constructor allowing to calculate the overall size of the provided path if it is a folder, but not the
     * file/folder permissions. When invoking any method related to file permissions an
     * {@link UnsupportedOperationException} is thrown.
     *
     * @param p the path
     * @param calcFolderSize if {@code true} the size of overall size of the folder will be calculated
     * @param fileAttributes the path's {@link BasicFileAttributes}
     * @param hasPosixAttrs whether the posix file attributes are provided
     * @throws IOException - If anything went wrong while calculating the folders size
     */
    public KNIMEFileAttributesWithoutPermissions(final Path p, final boolean calcFolderSize,
        final BasicFileAttributes fileAttributes, final boolean hasPosixAttrs) throws IOException {
        super(p, calcFolderSize, fileAttributes, hasPosixAttrs);
    }

    private static UnsupportedOperationException unsupportedOperation() {
        return new UnsupportedOperationException("Accessing the file permissions is not permitted.");
    }

    @Override
    Optional<Boolean> isReadable() {
        throw unsupportedOperation();
    }

    @Override
    Optional<Boolean> isWritable() {
        throw unsupportedOperation();
    }

    @Override
    Optional<Boolean> isExecutable() {
        throw unsupportedOperation();
    }
}
