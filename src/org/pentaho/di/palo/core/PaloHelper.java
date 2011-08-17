package org.pentaho.di.palo.core; 
/*
 *   This file is part of PaloKettlePlugin.
 *
 *   PaloKettlePlugin is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Lesser General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   PaloKettlePlugin is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   Copyright 2008 Stratebi Business Solutions, S.L.
 *   Copyright 2010 Pentaho
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.palo.api.Connection;
import org.palo.api.ConnectionConfiguration;
import org.palo.api.ConnectionFactory;
import org.palo.api.Consolidation;
import org.palo.api.Cube;
import org.palo.api.Database;
import org.palo.api.Dimension;
import org.palo.api.Element;
import org.palo.api.ElementNode;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseFactoryInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;



public class PaloHelper implements DatabaseFactoryInterface {

    public static boolean connectingToPalo = false;
    private Database database;
    private Connection connection;
    private DatabaseMeta databaseMeta;
    private final ListenersManager listeners;
    private PaloCubeCache cubeCache;


    public PaloHelper() {
    	this.listeners = new ListenersManager();
    }

    public PaloHelper(final DatabaseMeta databaseMeta) {
        this.databaseMeta = databaseMeta;
        this.listeners = new ListenersManager();
    }
    
    public String getConnectionTestReport(DatabaseMeta databaseMeta) throws KettleDatabaseException {
		StringBuffer report = new StringBuffer();
		
		PaloHelper helper = new PaloHelper(databaseMeta);
		try {
			helper.connect();
			
			// If the connection was successful
			//
			report.append("Connecting to PALO server [").append(databaseMeta.getName()).append("] went without a problem.").append(Const.CR);
			
		} catch (KettleException e) {
			report.append("Unable to connect to the PALO server: ").append(e.getMessage()).append(Const.CR);
			report.append(Const.getStackTracker(e));
		}
		finally
		{
			helper.disconnect();	
		}
		
		return report.toString();
    }

    /**
     * Connect to a PALO server
     * @throws KettleException In case something goes wrong
     */
    public final void connect() throws KettleException {
        while (PaloHelper.connectingToPalo) { 
            try {
                Thread.sleep(100);
            } catch(Exception ex) {}
        }
        assert databaseMeta != null;
        try {

            PaloHelper.connectingToPalo = true;
            
            ConnectionConfiguration connConfig = new ConnectionConfiguration(databaseMeta.environmentSubstitute(databaseMeta.getHostname()), databaseMeta.environmentSubstitute(databaseMeta.getDatabasePortNumberString()));
            connConfig.setUser(databaseMeta.environmentSubstitute(databaseMeta.getUsername()));
            connConfig.setPassword(databaseMeta.environmentSubstitute(databaseMeta.getPassword()));
            connConfig.setLoadOnDemand(true);
            connConfig.setTimeout(30000);
            connection = ConnectionFactory.getInstance().newConnection(connConfig);
            database = connection.getDatabaseByName(databaseMeta.environmentSubstitute(databaseMeta.getDatabaseName()));

            PaloHelper.connectingToPalo = false;

            if (database == null) {
                throw new KettleException("The specified database with name '" 
                        + databaseMeta.environmentSubstitute(databaseMeta.getDatabaseName())
                        + "' could not be found");
            }
        } catch (Exception e) {
            PaloHelper.connectingToPalo = false;
            
            throw new KettleException("Unexpected error while connecting to "
                    + "the Palo server: "+e.getMessage(), e);
        }
    }
    
    /**
     * @return a list of the names of the defined dimensions in the Palo 
     * server database
     * This is done 2 times for ordinary dimensions and then all others.
     */
    public final List < String > getDimensionsNames() {
        assert database != null;
        List < String > names = new ArrayList < String >();
        
        for (int i = 0; i < database.getDimensionCount(); i++) {
            Dimension dimension = database.getDimensionAt(i);

            //System.out.println(dimension.getName()+" Max Level "
            //+dimension.getMaxLevel()+ " Max depth "+dimension.getMaxDepth());
            if (!dimension.getName().startsWith("#")) {
//              if (!dimension.isAttributeDimension() &&
//                  !dimension.isSystemDimension() &&
//                  !dimension.isUserInfoDimension() &&
//                  !dimension.getDefaultHierarchy().isSubsetHierarchy() ) {
                 names.add(dimension.getName());
            }
        }

        for (int i = 0; i < database.getDimensionCount(); i++) {
            Dimension dimension = database.getDimensionAt(i);

            //System.out.println(dimension.getName()+" Max Level "
            //+dimension.getMaxLevel()+ " Max depth "+dimension.getMaxDepth());
            if (dimension.getName().startsWith("#")) {
                 names.add(dimension.getName());
            }
        }

        return names;
    }
    
    /**
     * @return a list of the names of the defined cubes in the Palo server 
     * database
     * This is done 2 times for ordinary cubes and then all others.
     */
    public final List < String > getCubesNames() {
        assert database != null;
        List < String > names = new ArrayList < String >();

        for (int i = 0; i < database.getCubeCount(); ++i) {
            Cube cube = database.getCubeAt(i);
            if (!cube.isAttributeCube() &&
                !cube.isSubsetCube() &&
                !cube.isSystemCube() &&
                !cube.isUserInfoCube() &&
                !cube.isViewCube()) {
                   names.add(cube.getName());
            }
        }

        for (int i = 0; i < database.getCubeCount(); ++i) {
            Cube cube = database.getCubeAt(i);
            if (cube.isAttributeCube() ||
                cube.isSubsetCube() ||
                cube.isSystemCube() ||
                cube.isUserInfoCube() ||
                cube.isViewCube()) {
                   names.add(cube.getName());
            }
        }

        return names;
    }

    
    public final List < String > getCubeDimensions(String cubeName) {
        assert database != null;
        List < String > cubeDimensions = new ArrayList < String >();

        Cube cube = database.getCubeByName(cubeName);
        Dimension[] dimensions = cube.getDimensions();
        for (int i = 0; i < dimensions.length; i++) {
            cubeDimensions.add(dimensions[i].getName());
        }
        return cubeDimensions;
    }
 
    public final RowMetaInterface getCellRowMeta(String cubeName, 
            List < DimensionField > fields, DimensionField cubeMeasure) throws KettleException {
        Cube cube = database.getCubeByName(cubeName);
        RowMetaInterface rowMeta = new RowMeta();
        
        Dimension[] dimensions = cube.getDimensions();
        for (int i = 0; i < dimensions.length; i++) {
            DimensionField df = null;
            for (DimensionField d : fields) {
                if(d.getDimensionName().equals(dimensions[i].getName())) {
                    df = d;
                    break;
                }
            }
            if (df == null) 
                throw new KettleException ("Dimension "+dimensions[i].getName()+" not found on  fields definition");
            if(df.getFieldType().equals("String"))
                rowMeta.addValueMeta(new ValueMeta(df.getFieldName(), ValueMetaInterface.TYPE_STRING));
            else {
                if(df.getFieldType().equals("Number")) 
                    rowMeta.addValueMeta(new ValueMeta(df.getFieldName(), ValueMetaInterface.TYPE_NUMBER));
                else
                    throw new KettleException("Only String and Number Types are acepted dimension fields");
            }
            
        }

        if (cubeMeasure == null) 
            throw new KettleException ("Measure field not defined.");
        if(cubeMeasure.getFieldType().equals("String"))
            rowMeta.addValueMeta(new ValueMeta(cubeMeasure.getFieldName(), ValueMetaInterface.TYPE_STRING));
        else {
            if(cubeMeasure.getFieldType().equals("Number")) 
                rowMeta.addValueMeta(new ValueMeta(cubeMeasure.getFieldName(), ValueMetaInterface.TYPE_NUMBER));
            else
                throw new KettleException("Only String and Number Types are acepted Measure field");
        }
        
        
        return rowMeta;
    }
    
    public final RowMetaInterface getDimensionRowMeta(String dimensionName, List < PaloDimensionLevel > levels, boolean baseElementsOnly) 
            throws KettleException {
        
    	Dimension dimension = database.getDimensionByName(dimensionName);

    	RowMetaInterface rowMeta = new RowMeta();
    	
    	if (baseElementsOnly && levels.size() != 1)
    		throw new KettleException("Base elements should only have one level defined.");
    	else if (baseElementsOnly == false && dimension.getDefaultHierarchy().getMaxLevel() + 1 != levels.size())
    		throw new KettleException("Levels of the dimension differ from defined levels");
    	
    	for ( int i = 0; i < levels.size(); i++) {
    		String fieldName = levels.get(i).getFieldName();
    		if (fieldName == null || fieldName == "")
    			fieldName = dimensionName;
    		int type = -1;
    		if(levels.get(i).getFieldType().equals("String"))
    			type = ValueMetaInterface.TYPE_STRING;
    		else {
    			if (levels.get(i).getFieldType().equals("Number"))
    				type = ValueMetaInterface.TYPE_NUMBER;
    			else
    				throw new KettleException("Only String and Number Types are acepted dimension fields");
    		}
    		rowMeta.addValueMeta(new ValueMeta(fieldName, type));
    	}
    	return rowMeta;

    }
   
    public final List <PaloDimensionLevel> getDimensionLevels(String dimensionName) throws KettleException {
        List < PaloDimensionLevel > levels = new ArrayList< PaloDimensionLevel >();
        Dimension dimension = database.getDimensionByName(dimensionName);
        if (dimension == null)
            throw new KettleException("Dimension "+dimensionName+" does not exist");

        for ( int i = 0; i <= dimension.getDefaultHierarchy().getMaxLevel(); i++) {
            levels.add(new PaloDimensionLevel("Level "+i,i,"",""));            
        }
        return levels;
    }

    public final List < Object[] > getDimensionRows(String dimensionName, RowMetaInterface rowMeta, boolean baseElementsOnly, final Listener listener) throws KettleException {
        assert database != null;
        final List < Object[] > rows = new ArrayList < Object[] >();
        
        final Dimension dimension = database.getDimensionByName(dimensionName);
        if (dimension == null) 
            throw new KettleException("Unable to find dimension '" + dimensionName + "' in the Palo database");

        if (baseElementsOnly){
        	for (Element element : dimension.getDefaultHierarchy().getElements()){
        		if (element.getChildCount() > 0)
        			continue;

        		Object[] row = {element.getName()};
        		rows.add(row);
        		this.listeners.oneMoreElement(element);
        	}
        }
        else{

        	// We take the maximum depth of the dimension?
        	// That's the number of fields we are going to have
        	// + the name of the Dimension itself (to make sure we have it all :-))
        	// + the attributes / elements themselves 
        	//
        	final int rowSize = dimension.getDefaultHierarchy().getMaxDepth() + 1;

        	// Loop over the elements...
        	//
        	final ElementNode[] elementsTree = dimension.getDefaultHierarchy().getElementsTree();

        	final Object[] row = new Object[rowSize];

        	this.listeners.prepareElements(elementsTree.length);
        	for (ElementNode node : elementsTree) {
        		// To debug
        		// this.showChildren(node);
        		this.assembleDimensionRows(rows, row, rowSize, dimension, node, 0, rowMeta, listener);
        		this.listeners.oneMoreElement(node);
        	}
        }
        
        return rows;
    }
    
    /**
     * Disconnect from the Palo server
     */
    public final void disconnect() {
        assert connection != null;
        if(this.connection != null)
            connection.disconnect();
    }

    /**
     * @return the databaseMeta
     */
    public final DatabaseMeta getDatabaseMeta() {
        return databaseMeta;
    }

    /**
     * @param databaseMeta the databaseMeta to set
     */
    public final void setDatabaseMeta(final DatabaseMeta databaseMeta) {
        this.databaseMeta = databaseMeta;
    }

    /**
     * @return the database
     */
    public final Database getDatabase() {
        return database;
    }

    /**
     * @param database the database to set
     */
    public final void setDatabase(final Database database) {
        this.database = database;
    }
    
    /**
     * Gets a list of consolidations for a given dimensions.
     */
    public final DimensionGroupingCollection getConsolidations(String dimensionName, 
            List < String[] > tableInputDimensions) throws Exception {
        if (tableInputDimensions.size() == 0)
            throw new Exception("Invalid Data. Number of rows must be > 0");
        int rowsCount = tableInputDimensions.size();
        int columnsCount = tableInputDimensions.get(0).length;
        if (columnsCount == 0)
            throw new Exception("Invalid data. Incoming column count must be > 0");
        
        DimensionGroupingCollection currentLevel = null;
        DimensionGroupingCollection previousLevel;
        int groupNumber = 0;
        
        for (int col = columnsCount - 2; col >= 0; col = col - 2) {
            previousLevel = currentLevel;
            currentLevel = new DimensionGroupingCollection();
           
            for (int row = 0; row < rowsCount; row++) {
                
            	String [] currentRow = tableInputDimensions.get(row);
                final String group = currentRow[col];
                final double consolidation = Double.parseDouble(currentRow[col + 1]);
                final int level = ((columnsCount - col) / 2) - 1;
                	
                final DimensionGrouping c;
                if (!currentLevel.contains(group, consolidation)) {
                    if (level == 0)
                    	c = new DimensionGrouping(group, dimensionName, level, consolidation);
                    else{
                        c = new DimensionGrouping(group, "Group ".concat(String.valueOf(groupNumber)), level, consolidation);
                        groupNumber++;
                     }
                     currentLevel.add(c);
                 } else {
                     c = currentLevel.find(group, consolidation);
                 }
                
                 if (level > 0) {
                     String childName = tableInputDimensions.get(row)[col + 2];
                     final double childConsolidation = Double.parseDouble(tableInputDimensions.get(row)[col + 3]);
                     if (!c.containsChild(childName, childConsolidation)) {
                         c.addChild(previousLevel.find(childName, childConsolidation));
                     }
                 }
            }
        }
        return currentLevel;
    }


    public final void addDimension(String dimensionName, DimensionGroupingCollection dimension, boolean createIfNotExists, boolean clearDimension, boolean clearConsolidations, boolean recreateDimension, boolean enableCache, boolean preloadCache, String elementType) throws Exception {
    	// if the dimension does not exist we create it
    	Dimension dim = database.getDimensionByName(dimensionName);
    	if (dim == null) {
    		if(createIfNotExists) {
    			dim = database.addDimension(dimensionName);
    		} else 
    			throw new KettleException("The dimension "+dimensionName + " does not exist.");
    	}

    	if (clearConsolidations){
    		ArrayList<Element> toDeleteArr = new ArrayList<Element>();

    		for (Element e : dim.getDefaultHierarchy().getElements())
    			if (e.getChildCount() > 0)
    				toDeleteArr.add(e);

    		if (toDeleteArr.size() > 0)
    			dim.getDefaultHierarchy().removeElements(toDeleteArr.toArray(new Element [0]));
    	}

    	if (recreateDimension){
    		database.removeDimension(database.getDimensionByName(dimensionName));
    		database.addDimension(dimensionName);
    	}	

    	// Takes ridiculously long for big dimensions.
    	if(clearDimension) {
    		Element [] elem = database.getDimensionByName(dimensionName).getDefaultHierarchy().getElements();
    		database.getDimensionByName(dimensionName).getDefaultHierarchy().removeElements(elem);
    	}
    	
    	PaloDimensionCache dimensionCache = new PaloDimensionCache(database, dimensionName, enableCache);
    	if (preloadCache)
    		dimensionCache.loadDimensionCache();
    	
    	for (DimensionGrouping d : dimension) 
    		this.addDimensionGrouping(dimensionCache, d, elementType);
    	
    }
    
    
    
    public final void removeDimension(String dimensionName) {
        Dimension dim = database.getDimensionByName(dimensionName);
        database.removeDimension(dim);
    }

    
    
    public final int removeCube(String cubeName) {
        Cube cube = database.getCubeByName(cubeName);
        if (cube == null)
        	return 0;
        database.removeCube(cube);
        return 1;
    }
    
    public final void createCube(String cubeName, String[] dimensionsNames) {
    	// If cube exist exit
    	Cube cube = database.getCubeByName(cubeName);
    	if (cube != null)
    		return;

        Dimension[] dims = new Dimension[dimensionsNames.length];
        for (int i = 0; i < dimensionsNames.length; i++) {
            dims[i] = database.getDimensionByName(dimensionsNames[i]);
        }
        database.addCube(cubeName, dims);
    }
    
    
    public final void clearCube(final String cubeName) throws Exception {
        Cube cube = database.getCubeByName(cubeName);
        if(cube == null)
            throw new Exception("The cube "+cubeName+" does not exist.");
        cube.clear();
    }

    public final void addCells(List < Object[] > cells) throws Exception {
    	if (cubeCache == null)
    		throw new Exception("Cube cache hasn't been initialized");
    	
    	int dimensionCount = cells.get(0).length - 1;
    	Element [][] dimensionRows = new Element [cells.size()][dimensionCount];
    	Object [] dataRows = new Object [cells.size()];
    	
    	Cube cube = cubeCache.getCube();
    	
    	if (cube.getDimensionCount() != dimensionCount)
    		throw new Exception("The cube does not have so many dimensions");
    	
    	/* For each row, populate the dimension and data rows */
    	for (int i = 0; i < cells.size(); i++) {
    		String[] elementNames = new String[dimensionCount];

    		/* Build dimension Array */
        	for (int j = 0; j < dimensionCount; j++) {
    			Element element = null;
    			Object elementNameObj = cells.get(i)[j];
    			
    			if (elementNameObj == null){
    				String row = "";
    				for (int k = 0; k < dimensionCount; k++)
    					row += "[" + (cells.get(i)[k] == null ? "NULL" : cells.get(i)[k].toString()) + "] ";
    				throw new Exception("Row: ("+row+") could not be added since it contains nulls in dimension " + (j + 1) + " (" + cube.getDimensionAt(j).getName() + ")");
    			}
    			
    			elementNames[j] = elementNameObj.toString();
    			
    			element = cubeCache.getElement(j, elementNames[j]);
    			
    			if(element == null)
					throw new Exception("The dimension element "+elementNames[j]+" does not exist in dimension "+cube.getDimensionAt(j).getName());
    			
    			dimensionRows[i][j] = element;
    		}
        	
    		/* Build Data Array */
    		Object dataValue = cells.get(i)[dimensionCount];
    		
    		if (!(dataValue instanceof Double) && !(dataValue instanceof String))
    			throw new Exception("Cell value must be a Double or String to write it to Palo.");
    		
    		dataRows[i] = dataValue;
    	}

    	/* Do a bulk commit with all rows passed to this procedure */
    	cube.setDataArray(dimensionRows, dataRows, Cube.SPLASHMODE_DEFAULT);
    	
    }
    
    /**
     * Gets all cells from Palo cube return them as a list of rows, and
     * each rows as an array of strings.
     */
    public final List < Object[] > getCells(final String cubeName) {
        final List < Object[] > rows = new ArrayList < Object[] >(); 
        final Listener listener = new Listener() {
                    private boolean stop;
                    private boolean cancel;
                    public void oneMoreElement(final Object element) {
                        rows.add((Object[]) element);
                    }
                    public void prepareElements(final int elements) {
                    }
                    public void stop() {
                        this.stop = true;
                    }
                    public void resume() {
                        this.stop = false;
                    }
                    public boolean getStop() {
                        return this.stop;
                    }
                    public void cancel() {
                        this.cancel = true;
                    }
                    public boolean getCancel() {
                        return this.cancel;
                    }
        };
        
        try {
            List<String> dimensions = this.getCubeDimensions(cubeName);
            List<DimensionField> fields = new ArrayList<DimensionField>();
            for(String s : dimensions) {
                fields.add(new DimensionField(s,s,"String"));
            }
            final RowMetaInterface rowMeta = this.getCellRowMeta(cubeName,fields,new DimensionField("Measure","Measure","Number"));
            this.getCells(cubeName, rowMeta, listener);
        } catch (Exception e) {
            return null;
        }
                
        return rows;
    }

    
    /**
     * Gets all elements from a Palo cube and notifies given listener to
     * process each element.
     */
    public final void getCells(final String cubeName, final RowMetaInterface rowMeta, final Listener listener) throws KettleException {
        Cube cube = database.getCubeByName(cubeName);
        Dimension[] cubeDimensions = cube.getDimensions();
        Element[][] cubeDimensionElements = new Element[cubeDimensions.length][];
        int[] currentDimensionIndexes = new int[cubeDimensions.length];
        
        int totalElements = 1;
        // we get all the elements of all the the dimensions 
        // related to the given cube
        for (int i = 0; i < cubeDimensions.length; i++) {
            Element[] elements = cubeDimensions[i].getDefaultHierarchy().getElements();
            // int leaves = 0;
            List < Element > list = new ArrayList < Element > ();
            for (int j = 0; j < elements.length; j++) {
                if (elements[j].getType() != Element.ELEMENTTYPE_CONSOLIDATED)
                    list.add(elements[j]);
            }
            cubeDimensionElements[i] = list.toArray(new Element[list.size()]);
            
            currentDimensionIndexes[i] = 0;
            totalElements *= cubeDimensionElements[i].length;
        }
        
        Element[] coordinates = new Element[cubeDimensions.length];
            
        this.listeners.prepareElements(totalElements);
        do {
            Object[] row = new Object[cubeDimensions.length + 1];
            for (int i = 0; i < cubeDimensions.length; i++) {
                //para cada dimension cojo el valor actual que queremos sacar
                coordinates[i] = cubeDimensionElements[i][
                        currentDimensionIndexes[i]];
                if(rowMeta.getValueMeta(i).isString()) {
                    row[i] = ((Element) coordinates[i]).getName();
                } else {
                    if(rowMeta.getValueMeta(i).isNumber()) {
                        row[i] = Double.parseDouble(((Element) coordinates[i]).getName());
                    } else 
                        throw new KettleException("Only String and Number Types are allowed for dimension values");
                }
                
            }
            Object data = cube.getData(coordinates);

            if(rowMeta.getValueMeta(cubeDimensions.length).isString()) {
                if (data instanceof String && data.toString().equals("")) {
                    //warning: no data
                } else {
                    row[cubeDimensions.length] = data.toString();
                    this.listeners.oneMoreElement(row);
                    listener.oneMoreElement(row);
                } 
            } else {
                if(rowMeta.getValueMeta(cubeDimensions.length).isNumber()) {
                    if (data instanceof Double) {
                        row[cubeDimensions.length] = data;
                        this.listeners.oneMoreElement(row);
                        listener.oneMoreElement(row);
                    } else {
                        if(data instanceof String && data.toString().equals("")) {
                            //no value
                        } else 
                            throw new KettleException("Measure field is defined as Number but data from palo is: "+ data.getClass().toString());
                    }
                } else 
                    throw new KettleException("Only String and Number Types are accepted for Measure Field");
            }
                
            if(listener.getStop()) {
                while (true) {
                    if(!listener.getStop() || listener.getCancel()) {
                        break;
                    }
                }
            }
            if(listener.getCancel())
                break;
        } while (iterateElements(currentDimensionIndexes, cubeDimensionElements));
    }

    public final void createDatabase(String databaseName) {
        assert database != null;
        this.connection.addDatabase(databaseName);
    }
    
   
    
    /**
     * Adds a listener to list of active PaloHelper listeners.
     */
    public final void addListener(final PaloHelper.Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Removes the given listener (if present) from the list of active
     * listeners.
     */
    public final void removeListener(final PaloHelper.Listener listener) {
        this.listeners.remove(listener);
    }


    

    /**
     * Listener of PaloHelper events.
     */
    public static interface Listener {
        /**
         * Signal notification of new element processed.
         */
        void oneMoreElement(final Object element);

        /**
         * Signal to prepare the listener to receive <i>maxNumberOfElements</i>
         * <code>oneMoreElement</code> calls.
         */
        void prepareElements(final int maxNumberOfElements);
        
        boolean getStop();
        
        void stop();
        void cancel();
        void resume();
        boolean getCancel();
    }

   
    private boolean isDimension(String elementName) {
        final List < String > dims = this.getDimensionsNames();
        for (final String s : dims) {
            if (s.equals(elementName)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unused")
	private void showChildren(final ElementNode node) {
        System.out.println("Leido un elemento: " + node.getElement().getName() + " level: " + node.getElement().getLevel() + " hijo de :" + node.getElement().getName());
        Element e = node.getElement();
        org.palo.api.Consolidation[] consolidations = e.getConsolidations();
        System.out.println("TYPE " + e.getTypeAsString());
        if (this.isDimension(e.getName()))
            System.out.println(e.getName() + " IS A DIMENSION");
        for (int i = 0; i < consolidations.length; i++)
            System.out.println("CONSOLIDATION: CHILD: " + consolidations[i].getChild().getName() + " PARENT: " + consolidations[i].getParent().getName());
        System.out.println("__________________________________________________");
        
        for (ElementNode childrenNode : node.getChildren()) {
            this.showChildren(childrenNode);
        }
    }
    
    /*
    private void assembleDimensionRowMeta(Dimension dimension, 
            RowMetaInterface rowMeta, ElementNode node, int depth) {
        ValueMetaInterface valueMeta = new ValueMeta("Depth "
                + depth, ValueMetaInterface.TYPE_STRING);
        rowMeta.addValueMeta(valueMeta);
        
        // More children?
        ElementNode[] children = node.getChildren();
        if (children != null && children.length > 0) {
            assembleDimensionRowMeta(dimension, rowMeta, children[0], depth + 1);
        }
    }
    */
    
    private void assembleDimensionRows(final List < Object[] > rows, 
            final Object[] currentRow, final int rowSize, 
            final Dimension dimension,  
            final ElementNode node, final int rowIndex,
            final RowMetaInterface rowMeta,
            final Listener listener) 
            throws KettleException {
        
        switch(node.getElement().getType()) {
            case Element.ELEMENTTYPE_STRING:
            case Element.ELEMENTTYPE_NUMERIC:
            case Element.ELEMENTTYPE_CONSOLIDATED:
                if(rowMeta.getValueMeta(rowIndex).isNumber()) {
                    try {
                        currentRow[rowIndex] = Double.parseDouble(node.getElement().getName());
                    } catch (Exception e) {
                        throw new KettleException("Failed to cast Palo Element to Number Type",e);
                    }
                } else {
                    if(rowMeta.getValueMeta(rowIndex).isString()) {
                        currentRow[rowIndex] = node.getElement().getName();
                    } else
                        throw new KettleException("Invalid Metadata type for dimension level. Must be Number or String");
                }
                break;
            default:
                throw new KettleException("Invalid Dimension Element Palo Type: "+node.getElement().getType());
        }
            
        
        // More children?
        ElementNode[] children = node.getChildren();
        if (children != null && children.length != 0) {
            for (int i = 0; i < children.length; i++) {
                assembleDimensionRows(rows, currentRow, rowSize, dimension, 
                        children[i], rowIndex + 1, rowMeta, listener);
            }
            /* After children has been filled, clean out the old child records otherwise shorter 
             * sibling hierarchies will hold on to these child entries. */
            for (int i = rowIndex;i < currentRow.length; i++)
                currentRow[i] = null;
        } else {
            Object[] clonedRow = new Object[currentRow.length];
            for (int i = 0;i < currentRow.length; i++) {
                clonedRow[i] = currentRow[i];
            }
            rows.add(clonedRow);
        }
    }

   
    private void addDimensionGrouping(PaloDimensionCache dimensionCache, DimensionGrouping dimensionGrouping, String elementType) throws KettleException {
    	// If this is a child dimension at the bottom of the tree
        if (dimensionGrouping.getChildren().size() == 0) {
        	try {
        		int paloElementType = (elementType.equals("Numeric") ? Element.ELEMENTTYPE_NUMERIC : Element.ELEMENTTYPE_STRING);
        		dimensionCache.createElement(dimensionGrouping.getName(), paloElementType, false);
            } 
        	catch (Exception e) {
                throw new KettleException("Failed to create element: "+ dimensionGrouping.getName(),e);
            }
            
        }
        // If this is a consolidated item, link the children 
        else {
            
        	// Add all child elements
        	for (DimensionGrouping c : dimensionGrouping.getChildren()) {
                this.addDimensionGrouping(dimensionCache, c, elementType);
            }
            
        	// Do consolidations
        	Consolidation[] finalConsolidations = null;
        	
            try {
            	String parentName = dimensionGrouping.getName();
                Element parentElement = dimensionCache.createElement(parentName, Element.ELEMENTTYPE_NUMERIC, false);

                Hashtable<String, Consolidation> newConsolidations = new Hashtable<String, Consolidation>();
                
                // Read current consolidations
                for (int i = 0; i < parentElement.getConsolidationCount(); i++) {
                	newConsolidations.put(parentElement.getConsolidationAt(i).getChild().getName(), parentElement.getConsolidationAt(i));
                }

                // See if new consolidations already exist with the correct weight.  If not, add to the list
                for (int i = 0; i < dimensionGrouping.getChildren().size(); i++) {
                	String childemename = dimensionGrouping.getChildren().get(i).getName();
                	if (childemename.equals(parentName))
                		continue;
                    	
                	Element childElement = dimensionCache.getElement(childemename);
                	
                	/* If the weight was changed, redo the consolidation with the correct weight */
                	if (newConsolidations.containsKey(childemename) 
                			&& newConsolidations.get(childemename).getWeight() != dimensionGrouping.getChildren().get(i).getConsolidationFactor())
                		newConsolidations.remove(childemename);
                    
                    if(!newConsolidations.containsKey(childemename)){
                    	Consolidation e = dimensionCache.getDimension().getDefaultHierarchy().newConsolidation(childElement, parentElement,  dimensionGrouping.getChildren().get(i).getConsolidationFactor());
                        newConsolidations.put(childemename, e);
                    }
                }
                
                // Copy consolidations into a structure suitable for updateConsolidations
                finalConsolidations = new Consolidation[newConsolidations.size()];
                int i = 0;
                
                ArrayList<String> array = new ArrayList<String>(newConsolidations.keySet());
                Collections.sort(array);
                for(String key : array){
                	finalConsolidations[i] = newConsolidations.get(key);
                	i++;
                }
                
                parentElement.updateConsolidations(finalConsolidations);
            } catch(Exception e) {
                throw new KettleException("failed to create consolidation: "+dimensionGrouping.getName(),e);
            }
        }
    }
    private boolean iterateElements(int[] currentDimensionIndexes, 
            Element[][] cubeDimensionElements) {
        for (int i = currentDimensionIndexes.length - 1; i >= 0; i--) {
           //we look for the element we have to iterate
           if (currentDimensionIndexes[i] + 1 
                        == cubeDimensionElements[i].length) {
              currentDimensionIndexes[i] = 0;
              if (i == 0) return false;
           } else {
              currentDimensionIndexes[i]++;
              break;
           }
        }
        return true;
    }

    public void loadCubeCache(String cubeName, boolean enableCache, boolean preloadCache) throws Exception{
    	this.cubeCache = new PaloCubeCache(this, cubeName, enableCache);

    	if (enableCache && preloadCache)
    		this.cubeCache.loadCubeCache();
    }
    
    public void clearCubeCache(){
    	this.cubeCache = null;
    }
    
    /**
     * Manager for list of listeners.
     * Simply a propagator of signals.
     */
    private final class ListenersManager {
        private final Set < Listener > listeners = new HashSet < Listener >();

        /**
         * Adds a new listener to the set of active listeners.
         */
        public void add(final Listener listener) { 
            this.listeners.add(listener); 
        }

        /**
         * Remove the listener.
         */
        public void remove(final Listener listener) { 
            this.listeners.remove(listener); 
        }

        /**
         * Propagates <i>oneMoreElement</i> signal.
         */
        public void oneMoreElement(final Object element) {
            for (final Listener l : listeners) l.oneMoreElement(element);
        }

        /**
         * Propagates <i>prepareElements</i> signal.
         */
        public void prepareElements(final int maxNumberOfElements) {
            for (final Listener l : listeners) {
                l.prepareElements(maxNumberOfElements);
            }
        }
    }
}

