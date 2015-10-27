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
package myservice.mynamespace.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAbstractEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.ex.ODataException;

public class DemoEdmProvider extends CsdlAbstractEdmProvider {

  // Service Namespace
  public static final String NAMESPACE = "OData.Demo";

  // EDM Container
  public static final String CONTAINER_NAME = "Container";
  public static final FullQualifiedName CONTAINER = new FullQualifiedName(NAMESPACE, CONTAINER_NAME);

  // Entity Types Names
  public static final String ET_PRODUCT_NAME = "Product";
//  public static final FullQualifiedName ET_PRODUCT_FQN = new FullQualifiedName(NAMESPACE, ET_PRODUCT_NAME);

  public static final String ET_CATEGORY_NAME = "Category";
  public static final FullQualifiedName ET_CATEGORY_FQN = new FullQualifiedName(NAMESPACE, ET_CATEGORY_NAME);

  // Entity Set Names
  public static final String ES_PRODUCTS_NAME = "Products";
  public static final String ES_CATEGORIES_NAME = "Categories";
  public static final String NAV_TO_CATEGORY = "Category";
  public static final String NAV_TO_PRODUCTS = "Products";
  private static int tableWidth; 
  private static int tableSize;
  private static int tableMumber;


  @Override
  public CsdlEntityType getEntityType(FullQualifiedName entityTypeName) {
	Map<String, CsdlProperty> map = new HashMap<String, CsdlProperty>();
    // this method is called for each EntityType that are configured in the Schema
    CsdlEntityType entityType = null;

    if (entityTypeName.toString().contains(ET_PRODUCT_NAME)) {
      // create EntityType properties
      map.put("id",new CsdlProperty().setName("ID")
          .setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName()));
	  for (int i = 2; i < tableWidth; i++) {		  
    	  map.put("Col" + i, new CsdlProperty().setName("Col" + i).setType(EdmPrimitiveTypeKind.String.getFullQualifiedName()));
      }
	  map.put("key",new CsdlProperty().setName("Key")
          .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName()));

      // create PropertyRef for Key element
      CsdlPropertyRef propertyRef = new CsdlPropertyRef();
      propertyRef.setName("ID");

      // configure EntityType
      entityType = new CsdlEntityType();
      entityType.setName(entityTypeName.getName());
      entityType.setProperties(new ArrayList(map.values()));
      entityType.setKey(Arrays.asList(propertyRef));
      // entityType.setNavigationProperties(navPropList);

    } else if (entityTypeName.equals(ET_CATEGORY_FQN)) {
      // create EntityType properties
      CsdlProperty id = new CsdlProperty().setName("ID")
          .setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());
      CsdlProperty name = new CsdlProperty().setName("Name")
          .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
      CsdlProperty Description = new CsdlProperty().setName("Description")
              .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
      // create PropertyRef for Key element
      CsdlPropertyRef propertyRef = new CsdlPropertyRef();
      propertyRef.setName("ID");

      // configure EntityType
      entityType = new CsdlEntityType();
      entityType.setName(ET_CATEGORY_NAME);
      entityType.setProperties(Arrays.asList(id, name));
      entityType.setKey(Arrays.asList(propertyRef));
      //entityType.setNavigationProperties(navPropList);
    }

    return entityType;

  }

  @Override
  public CsdlEntitySet getEntitySet(FullQualifiedName entityContainer, String entitySetName) {

    CsdlEntitySet entitySet = null;

    if (entityContainer.equals(CONTAINER)) {

      if (entitySetName.contains(ES_PRODUCTS_NAME)) {

        entitySet = new CsdlEntitySet();
        entitySet.setName(ES_PRODUCTS_NAME + getIndex(entitySetName));
        entitySet.setType(new FullQualifiedName(NAMESPACE, ET_PRODUCT_NAME + "-" + getIndex(entitySetName) + "-" + tableWidth + "-" + tableSize));

        // navigation
//        CsdlNavigationPropertyBinding navPropBinding = new CsdlNavigationPropertyBinding();
//        navPropBinding.setTarget("Categories"); // the target entity set, where the navigation property points to
//        navPropBinding.setPath("Category"); // the path from entity type to navigation property
//        List<CsdlNavigationPropertyBinding> navPropBindingList = new ArrayList<CsdlNavigationPropertyBinding>();
//        navPropBindingList.add(navPropBinding);
//        entitySet.setNavigationPropertyBindings(navPropBindingList);

      } else if (entitySetName.equals(ES_CATEGORIES_NAME)) {

        entitySet = new CsdlEntitySet();
        entitySet.setName(ES_CATEGORIES_NAME);
        entitySet.setType(ET_CATEGORY_FQN);

//        // navigation
//        CsdlNavigationPropertyBinding navPropBinding = new CsdlNavigationPropertyBinding();
//        navPropBinding.setTarget("Products"); // the target entity set, where the navigation property points to
//        navPropBinding.setPath("Products"); // the path from entity type to navigation property
//        List<CsdlNavigationPropertyBinding> navPropBindingList = new ArrayList<CsdlNavigationPropertyBinding>();
//        navPropBindingList.add(navPropBinding);
//        entitySet.setNavigationPropertyBindings(navPropBindingList);
      }
    }

    return entitySet;
  }

  @Override
  public CsdlEntityContainerInfo getEntityContainerInfo(FullQualifiedName entityContainerName) {

    // This method is invoked when displaying the service document at
    // e.g. http://localhost:8080/DemoService/DemoService.svc
    if (entityContainerName == null || entityContainerName.equals(CONTAINER)) {
      CsdlEntityContainerInfo entityContainerInfo = new CsdlEntityContainerInfo();
      entityContainerInfo.setContainerName(CONTAINER);
      return entityContainerInfo;
    }

    return null;
  }

  @Override
  public List<CsdlSchema> getSchemas() throws ODataException {
    // create Schema
    CsdlSchema schema = new CsdlSchema();
    schema.setNamespace(NAMESPACE);

    // add EntityTypes
    List<CsdlEntityType> entityTypes = new ArrayList<CsdlEntityType>();
    
    for(int num = 1; num <= tableMumber; num++){
    	entityTypes.add(getEntityType(new FullQualifiedName(NAMESPACE, ET_PRODUCT_NAME + "-" + num + "-" + tableWidth + "-" + tableSize)));
    }
    entityTypes.add(getEntityType(ET_CATEGORY_FQN));
    schema.setEntityTypes(entityTypes);

    // add EntityContainer
    schema.setEntityContainer(getEntityContainer());

    // finally
    List<CsdlSchema> schemas = new ArrayList<CsdlSchema>();
    schemas.add(schema);

    return schemas;
  }

  @Override
  public CsdlEntityContainer getEntityContainer() throws ODataException {

    // create EntitySets
    List<CsdlEntitySet> entitySets = new ArrayList<CsdlEntitySet>();
    
    for(int num = 1; num <= tableMumber; num++){   	
    	entitySets.add(getEntitySet(CONTAINER, ES_PRODUCTS_NAME + num));   	
    }
    entitySets.add(getEntitySet(CONTAINER, ES_CATEGORIES_NAME));

    // create EntityContainer
    CsdlEntityContainer entityContainer = new CsdlEntityContainer();
    entityContainer.setName(CONTAINER_NAME);
    entityContainer.setEntitySets(entitySets);

    return entityContainer;
  }
  
  public static void setTableWidth(int width){
	  tableWidth = width;
  }
  public static void setTableSize(int size){
	  tableSize = size;
  }
  public static void setTableNumber(int number){
	  tableMumber = number;
  }
  public int getIndex(String edmEntitySetName){
	  int size = ES_PRODUCTS_NAME.length();	  
	  return Integer.valueOf(edmEntitySetName.substring(size));
  }

}
