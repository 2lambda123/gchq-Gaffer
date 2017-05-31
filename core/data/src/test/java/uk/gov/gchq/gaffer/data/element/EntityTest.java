/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.data.element;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class EntityTest extends ElementTest {

    @Override
    @Test
    public void shouldSetAndGetFields() {
        // Given
        final Entity entity = new Entity.Builder().group("group")
                                                  .vertex("identifier")
                                                  .build();

        // Then
        assertEquals("group", entity.getGroup());
        assertEquals("identifier", entity.getVertex());
    }

    @Test
    public void shouldBuildEntity() {
        // Given
        final String vertex = "vertex1";
        final String propValue = "propValue";

        // When
        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(vertex)
                .property(TestPropertyNames.STRING, propValue)
                .build();

        // Then
        assertEquals(TestGroups.ENTITY, entity.getGroup());
        assertEquals(vertex, entity.getVertex());
        assertEquals(propValue, entity.getProperty(TestPropertyNames.STRING));
    }

    @Test
    public void shouldConstructEntity() {
        // Given
        final String vertex = "vertex1";
        final String propValue = "propValue";

        // When
        final Entity entity = new Entity.Builder().group(TestGroups.ENTITY)
                                                  .vertex(vertex)
                                                  .property(TestPropertyNames.STRING, propValue)
                                                  .build();

        // Then
        assertEquals(TestGroups.ENTITY, entity.getGroup());
        assertEquals(vertex, entity.getVertex());
        assertEquals(propValue, entity.getProperty(TestPropertyNames.STRING));
    }

    @Test
    public void shouldCloneEntity() {
        // Given
        final String vertex = "vertex1";
        final String propValue = "propValue";

        // When
        final Entity entity = new Entity(TestGroups.ENTITY, vertex);
        final Entity clone = entity.emptyClone();

        // Then
        assertEquals(clone, entity);
    }

    @Override
    @Test
    public void shouldReturnTrueForEqualsWithTheSameInstance() {
        // Given
        final Entity entity = new Entity.Builder().group("group")
                                                  .vertex("identifier")
                                                  .build();

        // When
        boolean isEqual = entity.equals(entity);

        // Then
        assertTrue(isEqual);
        assertEquals(entity.hashCode(), entity.hashCode());
    }

    @Override
    @Test
    public void shouldReturnTrueForEqualsWhenAllCoreFieldsAreEqual() {
        // Given
        final Entity entity1 = new Entity.Builder().group("group")
                                                   .vertex("identifier")
                                                   .property("some property", "some value")
                                                   .build();

        final Entity entity2 = cloneCoreFields(entity1).build();
        entity2.putProperty("some property", "some value");

        // When
        boolean isEqual = entity1.equals((Object) entity2);

        // Then
        assertTrue(isEqual);
        assertEquals(entity1.hashCode(), entity2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenPropertyIsDifferent() {
        // Given
        final Entity entity1 = new Entity.Builder().group("group")
                                                   .vertex("identifier")
                                                   .property("some property", "some value")
                                                   .build();

        final Entity entity2 = cloneCoreFields(entity1).build();
        entity2.putProperty("some property", "some other value");

        // When
        boolean isEqual = entity1.equals((Object) entity2);

        // Then
        assertFalse(isEqual);
        assertNotEquals(entity1.hashCode(), entity2.hashCode());
    }

    @Override
    @Test
    public void shouldReturnFalseForEqualsWhenGroupIsDifferent() {
        // Given
        final Entity entity1 = new Entity.Builder().group("group")
                                                   .vertex("vertex")
                                                   .build();

        final Entity entity2 = new Entity.Builder().group("a different group")
                                                   .vertex(entity1.getVertex())
                                                   .build();

        // When
        boolean isEqual = entity1.equals((Object) entity2);

        // Then
        assertFalse(isEqual);
        assertFalse(entity1.hashCode() == entity2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenIdentifierIsDifferent() {
        // Given
        final Entity entity1 = new Entity.Builder().group("group")
                                                   .vertex("vertex")
                                                   .build();

        final Entity entity2 = cloneCoreFields(entity1).vertex("different vertex")
                                                       .build();

        // When
        boolean isEqual = entity1.equals((Object) entity2);

        // Then
        assertFalse(isEqual);
        assertFalse(entity1.hashCode() == entity2.hashCode());
    }

    @Override
    @Test
    public void shouldSerialiseAndDeserialiseIdentifiers() throws SerialisationException {
        // Given
        final Entity entity = new Entity.Builder().group("group")
                                                  .vertex(1L)
                                                  .build();

        final JSONSerialiser serialiser = new JSONSerialiser();

        // When
        final byte[] serialisedElement = serialiser.serialise(entity);
        final Entity deserialisedElement = serialiser.deserialise(serialisedElement, entity
                .getClass());

        // Then
        assertEquals(entity, deserialisedElement);
    }

    @Override
    protected Entity newElement(final String group) {
        return new Entity.Builder().group(group)
                                   .build();
    }

    @Override
    protected Entity newElement() {
        return new Entity.Builder().build();
    }

    private Entity.Builder cloneCoreFields(final Entity entity) {
        return new Entity.Builder().group(entity.getGroup())
                                   .vertex(entity.getVertex());
    }
}
