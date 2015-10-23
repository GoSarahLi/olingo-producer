/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package myservice.mynamespace.data;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import myservice.mynamespace.service.DemoEdmProvider;
import myservice.mynamespace.util.Util;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmKeyPropertyRef;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpMethod;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriParameter;

public class Storage {

  // represent our database
  private List<List<Entity>> productLists;
  private List<Entity> categoryList;
  private final static Integer TABLE_NUMBER = 10;
  private final static Integer TABLE_SIZE = 1024;
  private final static Integer TABLE_WIDTH = 128;
  public Storage() {
	  
    productLists = new ArrayList<List<Entity>>();
    categoryList = new ArrayList<Entity>();

    // creating some sample data
    initProductSampleData();
    initCategorySampleData();
  }

  /* PUBLIC FACADE */

  public EntityCollection readEntitySetData(EdmEntitySet edmEntitySet) {
    EntityCollection entitySet = null;

    if (edmEntitySet.getName().contains(DemoEdmProvider.ES_PRODUCTS_NAME)) {
      entitySet = getProducts(getIndex(edmEntitySet.getName()));
    } else if (edmEntitySet.getName().equals(DemoEdmProvider.ES_CATEGORIES_NAME)) {
      entitySet = getCategories();
    }

    return entitySet;
  }

  public Entity readEntityData(EdmEntitySet edmEntitySet, List<UriParameter> keyParams) {
    Entity entity = null;

    EdmEntityType edmEntityType = edmEntitySet.getEntityType();

    if (edmEntityType.getName().contains(DemoEdmProvider.ET_PRODUCT_NAME)) {
      entity = getProduct(edmEntityType, keyParams);
    } else if (edmEntityType.getName().equals(DemoEdmProvider.ET_CATEGORY_NAME)) {
      entity = getCategory(edmEntityType, keyParams);
    }

    return entity;
  }

  // Navigation

  public Entity getRelatedEntity(Entity entity, EdmEntityType relatedEntityType) {
    EntityCollection collection = getRelatedEntityCollection(entity, relatedEntityType);
    if (collection.getEntities().isEmpty()) {
      return null;
    }
    return collection.getEntities().get(0);
  }

  public Entity getRelatedEntity(Entity entity, EdmEntityType relatedEntityType, List<UriParameter> keyPredicates) {

    EntityCollection relatedEntities = getRelatedEntityCollection(entity, relatedEntityType);
    return Util.findEntity(relatedEntityType, relatedEntities, keyPredicates);
  }

  public EntityCollection getRelatedEntityCollection(Entity sourceEntity, EdmEntityType targetEntityType) {
//    EntityCollection navigationTargetEntityCollection = new EntityCollection();
//
//    FullQualifiedName relatedEntityFqn = targetEntityType.getFullQualifiedName();
//    String sourceEntityFqn = sourceEntity.getType();
//
//    if (sourceEntityFqn.contains(DemoEdmProvider.ES_PRODUCTS_NAME)
//        && relatedEntityFqn.equals(DemoEdmProvider.ET_CATEGORY_FQN)) {
//      navigationTargetEntityCollection.setId(createId(sourceEntity, "ID", DemoEdmProvider.NAV_TO_CATEGORY));
//      // relation Products->Category (result all categories)
//      int productID = (Integer) sourceEntity.getProperty("ID").getValue();
//      for(int i = 0; i < 10; i ++){
//    	  navigationTargetEntityCollection.getEntities().add(categoryList.get(i));
//      }
      
//    } else if (sourceEntityFqn.equals(DemoEdmProvider.ET_CATEGORY_FQN.getFullQualifiedNameAsString())
//        && relatedEntityFqn.equals(DemoEdmProvider.ET_PRODUCT_FQN)) {
//      navigationTargetEntityCollection.setId(createId(sourceEntity, "ID", DemoEdmProvider.NAV_TO_PRODUCTS));
//      // relation Category->Products (result all products)
//      int categoryID = (Integer) sourceEntity.getProperty("ID").getValue();
//      if (categoryID == 1) {
//        // the first 2 products are notebooks
//        navigationTargetEntityCollection.getEntities().addAll(productList.subList(0, 2));
//      } else if (categoryID == 2) {
//        // the next 2 products are organizers
//        navigationTargetEntityCollection.getEntities().addAll(productList.subList(2, 4));
//      } else if (categoryID == 3) {
//        // the first 2 products are monitors
//        navigationTargetEntityCollection.getEntities().addAll(productList.subList(4, 6));
//      }
//    }
//
//    if (navigationTargetEntityCollection.getEntities().isEmpty()) {
//      return null;
//    }
//
    return null;
  }

  public Entity createEntityData(EdmEntitySet edmEntitySet, Entity entityToCreate) {

    EdmEntityType edmEntityType = edmEntitySet.getEntityType();

    // actually, this is only required if we have more than one Entity Type
    if (edmEntityType.getName().contains(DemoEdmProvider.ET_PRODUCT_NAME)) {
      return createProduct(edmEntityType, entityToCreate, getIndex(edmEntitySet.getName()));
    }

    return null;
  }

  /**
   * This method is invoked for PATCH or PUT requests
   * */
  public void updateEntityData(EdmEntitySet edmEntitySet, List<UriParameter> keyParams, Entity updateEntity,
      HttpMethod httpMethod) throws ODataApplicationException {

    EdmEntityType edmEntityType = edmEntitySet.getEntityType();

    // actually, this is only required if we have more than one Entity Type
    if (edmEntityType.getName().contains(DemoEdmProvider.ET_PRODUCT_NAME)) {
      updateProduct(edmEntityType, keyParams, updateEntity, httpMethod, getIndex(edmEntitySet.getName()));
    }
  }

  public void deleteEntityData(EdmEntitySet edmEntitySet, List<UriParameter> keyParams)
      throws ODataApplicationException {

    EdmEntityType edmEntityType = edmEntitySet.getEntityType();

    // actually, this is only required if we have more than one Entity Type
    if (edmEntityType.getName().contains(DemoEdmProvider.ET_PRODUCT_NAME)) {
      deleteProduct(edmEntityType, keyParams, getIndex(edmEntitySet.getName()));
    }
  }
  
  /* INTERNAL */

  private EntityCollection getProducts(int index) {
    EntityCollection retEntitySet = new EntityCollection();

    for (Entity productEntity : this.productLists.get(index - 1)) {
      retEntitySet.getEntities().add(productEntity);
    }

    return retEntitySet;
  }

  private Entity getProduct(EdmEntityType edmEntityType, List<UriParameter> keyParams) {

    // the list of entities at runtime
    EntityCollection entityCollection = getProducts(getIndexfromType(edmEntityType.getName()));

    /* generic approach to find the requested entity */
    return Util.findEntity(edmEntityType, entityCollection, keyParams);
  }

  private EntityCollection getCategories() {
    EntityCollection entitySet = new EntityCollection();

    for (Entity categoryEntity : this.categoryList) {
      entitySet.getEntities().add(categoryEntity);
    }

    return entitySet;
  }

  private Entity getCategory(EdmEntityType edmEntityType, List<UriParameter> keyParams) {

    // the list of entities at runtime
    EntityCollection entitySet = getCategories();

    /* generic approach to find the requested entity */
    return Util.findEntity(edmEntityType, entitySet, keyParams);
  }
  
  private void updateProduct(EdmEntityType edmEntityType, List<UriParameter> keyParams, Entity entity,
      HttpMethod httpMethod, int index) throws ODataApplicationException {

    Entity productEntity = getProduct(edmEntityType, keyParams);
    if (productEntity == null) {
      throw new ODataApplicationException("Entity not found", HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
    }

    // loop over all properties and replace the values with the values of the given payload
    // Note: ignoring ComplexType, as we don't have it in our odata model
    List<Property> existingProperties = productEntity.getProperties();
    for (Property existingProp : existingProperties) {
      String propName = existingProp.getName();

      // ignore the key properties, they aren't updateable
      if (isKey(edmEntityType, propName)) {
        continue;
      }

      Property updateProperty = entity.getProperty(propName);
      // the request payload might not consider ALL properties, so it can be null
      if (updateProperty == null) {
        // if a property has NOT been added to the request payload
        // depending on the HttpMethod, our behavior is different
        if (httpMethod.equals(HttpMethod.PATCH)) {
          // as of the OData spec, in case of PATCH, the existing property is not touched
          continue; // do nothing
        } else if (httpMethod.equals(HttpMethod.PUT)) {
          // as of the OData spec, in case of PUT, the existing property is set to null (or to default value)
          existingProp.setValue(existingProp.getValueType(), null);
          continue;
        }
      }

      // change the value of the properties
      existingProp.setValue(existingProp.getValueType(), updateProperty.getValue());
    }
  }

  private void deleteProduct(EdmEntityType edmEntityType, List<UriParameter> keyParams, int index)
      throws ODataApplicationException {

    Entity productEntity = getProduct(edmEntityType, keyParams);
    if (productEntity == null) {
      throw new ODataApplicationException("Entity not found", HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
    }

    this.productLists.get(index).remove(productEntity);
  }
  
  private Entity createProduct(EdmEntityType edmEntityType, Entity entity, int index) {

    // the ID of the newly created product entity is generated automatically
    int newId = 1;
    while (productIdExists(newId, index)) {
      newId++;
    }

    Property idProperty = entity.getProperty("ID");
    if (idProperty != null) {
      idProperty.setValue(ValueType.PRIMITIVE, Integer.valueOf(newId));
    } else {
      // as of OData v4 spec, the key property can be omitted from the POST request body
      entity.getProperties().add(new Property(null, "ID", ValueType.PRIMITIVE, newId));
    }
    entity.setId(createId(entity, "ID"));
    this.productLists.get(index-1).add(entity);

    return entity;

  }

  private boolean productIdExists(int id, int index) {

    for (Entity entity : this.productLists.get(index)) {
      Integer existingID = (Integer) entity.getProperty("ID").getValue();
      if (existingID.intValue() == id) {
        return true;
      }
    }

    return false;
  }
  
  /* HELPER */
  
  private boolean isKey(EdmEntityType edmEntityType, String propertyName) {
    List<EdmKeyPropertyRef> keyPropertyRefs = edmEntityType.getKeyPropertyRefs();
    for (EdmKeyPropertyRef propRef : keyPropertyRefs) {
      String keyPropertyName = propRef.getName();
      if (keyPropertyName.equals(propertyName)) {
        return true;
      }
    }
    return false;
  }
  
  private void initProductSampleData() {
	  	DemoEdmProvider.setTableWidth(TABLE_WIDTH);
	  	DemoEdmProvider.setTableSize(TABLE_SIZE);
	  	
	  	for(int num = 1; num <= TABLE_NUMBER; num++){
	  		ArrayList<Entity> productList = new ArrayList<Entity>();
	  		for(int row = 1; row <= TABLE_SIZE; row++){
	  			Entity entity = new Entity();
	  			entity.addProperty(new Property(null, "Key", ValueType.PRIMITIVE, "key" + row));
	  			entity.addProperty(new Property(null, "ID", ValueType.PRIMITIVE, row));
	  			for (int i = 2; i < TABLE_WIDTH; i++) {		  
	  				entity.addProperty(new Property(null, "Col" + i, ValueType.PRIMITIVE, "Col" + i + 0));
		  		}
	  			FullQualifiedName ET_PRODUCT_FQN = new FullQualifiedName(DemoEdmProvider.NAMESPACE, DemoEdmProvider.ET_PRODUCT_NAME + "-" + num + "-" + TABLE_WIDTH + "-" + TABLE_SIZE);
	  			entity.setType(ET_PRODUCT_FQN.getFullQualifiedNameAsString());
	  			entity.setId(createId(entity, "ID"));
	  			productList.add(entity);
	  		}
	  		productLists.add(productList);
	  	}
  }

  private void initCategorySampleData() {
	    for (int row = 1; row <= TABLE_NUMBER; row++) {
	        Entity entity = new Entity();
	        entity.addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "name" + row));
	        entity.addProperty(new Property(null, "ID", ValueType.PRIMITIVE, row));
	        entity.addProperty(new Property(null, "Description", ValueType.PRIMITIVE, "Description" + row));
	        entity.setType(DemoEdmProvider.ET_CATEGORY_FQN.getFullQualifiedNameAsString());
	        entity.setId(createId(entity, "ID"));
	        categoryList.add(entity);
	    }
  }

  private URI createId(Entity entity, String idPropertyName) {
    return createId(entity, idPropertyName, null);
  }

  private URI createId(Entity entity, String idPropertyName, String navigationName) {
    try {
      StringBuilder sb = new StringBuilder(getEntitySetName(entity)).append("(");
      final Property property = entity.getProperty(idPropertyName);
      sb.append(property.asPrimitive()).append(")");
      if(navigationName != null) {
        sb.append("/").append(navigationName);
      }
      return new URI(sb.toString());
    } catch (URISyntaxException e) {
      throw new ODataRuntimeException("Unable to create (Atom) id for entity: " + entity, e);
    }
  }

  private String getEntitySetName(Entity entity) {
    if(DemoEdmProvider.ET_CATEGORY_FQN.getFullQualifiedNameAsString().equals(entity.getType())) {
      return DemoEdmProvider.ES_CATEGORIES_NAME;
//    } else if(DemoEdmProvider.ET_PRODUCT_FQN.getFullQualifiedNameAsString().equals(entity.getType())) {
//      return DemoEdmProvider.ES_PRODUCTS_NAME;
    }
    return entity.getType();
  }
  public int getIndex(String edmEntitySetName){
	  int size = DemoEdmProvider.ES_PRODUCTS_NAME.length();	  
	  return Integer.valueOf(edmEntitySetName.substring(size));
  }
  public int getIndexfromType(String edmEntityTypeName){
	  String[] strs = edmEntityTypeName.split("-");
	  return Integer.valueOf(strs[1]);
  }
}
