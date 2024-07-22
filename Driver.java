package uga.cs4370.mydbimpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import uga.cs4370.mydb.Cell;
import uga.cs4370.mydb.Predicate;
import uga.cs4370.mydb.RA;
import uga.cs4370.mydb.Relation;
import uga.cs4370.mydb.RelationBuilder;
import uga.cs4370.mydb.Type;

public class Driver implements RA {
    // Result row limit
    private static final int ROW_LIMIT = 50;

    public static void main(String[] args) {
        // Following is an example of how to use the relation class.
        // This creates a table with three columns with below mentioned
        // column names and data types.
        // After creating the table, data is loaded from a CSV file.
        // Path should be replaced with a correct file path for a compatible
        // CSV file.
        // Example myID
        // String myid = "esp94422";

        // Folder path to the CSV file.
        String csvFolderPath = "/var/lib/mysql-files/";

        // Create a relation for Instructor
        Relation rel1 = new RelationBuilder()
                .attributeNames(List.of("id", "name", "dept_name", "salary"))
                .attributeTypes(List.of(Type.INTEGER, Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        rel1.loadData(csvFolderPath+"instructor_export.csv");
        // rel1.print();

        // Create a relation for Student
        Relation rel2 = new RelationBuilder()
                .attributeNames(List.of("id", "name", "dept_name", "tot_cred"))
                .attributeTypes(List.of(Type.INTEGER, Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        rel2.loadData(csvFolderPath + "student_export.csv");
        // rel2.print();

        // Select all the rows from relation 1 where salary is greater than 120000
        Relation filteredRelation = new Driver().select(rel1, row -> (row.get(3).getAsDouble()) > 120000);
        System.out.println("\n****************************************************************************");
        System.out.println("1) Select Example");
        System.out.println("select * from instructor where salary > 120000  limit 50;");
        System.out.println("****************************************************************************");
        filteredRelation.print();

        // Project the relation rel1 to only include the name and dept_name attributes
        // from rel1 Instructor
        List<String> inst_attrs = List.of("name", "dept_name");
        Relation inst_projectedRelation = new Driver().project(rel1, inst_attrs);
        System.out.println("\n****************************************************************************");
        System.out.println("2A) Project Example 1");
        System.out.println("select name,dept_name from instructor limit 50;");
        System.out.println("****************************************************************************");

        inst_projectedRelation.print();

        // Project the relation rel1 to only include the name and dept_name attributes
        // from rel3 Student
        List<String> stud_attrs = List.of("name", "dept_name");
        Relation stud_projectedRelation = new Driver().project(rel2, stud_attrs);
        System.out.println("\n****************************************************************************");
        System.out.println("2B) Project Example 2");
        System.out.println("select name,dept_name from student limit 50;");
        System.out.println("****************************************************************************");
        stud_projectedRelation.print();

        // Union the relation inst_projectedRelation and stud_projectedRelation
        Relation unionRelation = new Driver().union(inst_projectedRelation, stud_projectedRelation);
        System.out.println("\n****************************************************************************");
        System.out.println("3) Union Example");
        System.out
                .println("SELECT name, dept_name FROM instructor UNION SELECT name, dept_name FROM student LIMIT 50;");
        System.out.println("****************************************************************************");
        unionRelation.print();

        // Difference the relation Duplicate instructor and original instructor
        Relation diffRelation = new Driver().diff(inst_projectedRelation, stud_projectedRelation);
        System.out.println("\n****************************************************************************");
        System.out.println("4) Diff Example");
        System.out
                .println(
                        "SELECT name, dept_name FROM instructor EXCEPT  SELECT name, dept_name FROM student LIMIT 50;");
        System.out.println("****************************************************************************");
        diffRelation.print();

        // Rename the attributes in rel1 to corresponding names in renamedAttr
        Relation renamedRelation = new Driver().rename(rel1, List.of("id", "name", "dept_name", "salary"),
                List.of("instructor_id", "instructor_name", "instructor_dept_name", "instructor_salary"));
        System.out.println("\n****************************************************************************");
        System.out.println("5) Rename Example");
        System.out
                .println(
                        "SELECT  id AS instructor_id, name AS instructor_name, dept_name AS  instructor_dept_name , salary as instructor_salary  FROM instructor LIMIT 50;");
        System.out.println("****************************************************************************");
        renamedRelation.print();

    }

    /**
     * Select all the rows from relation rel where p is true.
     * 
     * @param rel The relation from which to select rows.
     * @param p   The predicate to apply.
     * @return The resulting relation after applying the select operation.
     * 
     */
    @Override
    public Relation select(Relation rel, Predicate p) {
        Relation rel_selected = new RelationBuilder()
                .attributeNames(rel.getAttrs())
                .attributeTypes(rel.getTypes())
                .build();

        int counter = 0;
        for (int i = 0; i < rel.getSize() && counter < ROW_LIMIT; i++) {
            if (p.check(rel.getRow(i))) {
                rel_selected.insert(rel.getRow(i));
                counter++;
            }
        }

        return rel_selected;
    }

    /**
     * Project the relation rel to only include the attributes in attrs.
     * 
     * @param rel   The relation from which to project.
     * @param attrs The attributes to project.
     * @throws IllegalArgumentException If attributes in attrs are not present in
     *                                  rel.
     * @return The resulting relation after applying the project operation.
     */
    @Override
    public Relation project(Relation rel, List<String> attrs) {
        // Determine the types of the selected attributes.
        List<Type> selectedTypes = new ArrayList<>();
        for (String attr : attrs) {
            int index = rel.getAttrs().indexOf(attr);
            if (index != -1) {
                selectedTypes.add(rel.getTypes().get(index));
            } else {
                throw new IllegalArgumentException("Attribute not found: " + attr);
            }
        }

        Relation projectedRelation = new RelationBuilder()
                .attributeNames(attrs)
                .attributeTypes(selectedTypes)
                .build();

        int counter = 0;
        for (int i = 0; i < rel.getSize() && counter < ROW_LIMIT; i++) {
            List<Cell> newRow = new ArrayList<>();
            for (String attr : attrs) {
                int index = rel.getAttrs().indexOf(attr);
                newRow.add(rel.getRow(i).get(index));
            }
            projectedRelation.insert(newRow);
            counter++;
        }

        return projectedRelation;
    }

    /**
     * Union the relation rel1 and rel2
     * 
     * @param rel1 The first relation.
     * @param rel2 The second relation.
     * @return The resulting relation after applying the union operation.
     * @throws IllegalArgumentException If rel1 and rel2 are not compatible.
     */

    @Override
    public Relation union(Relation rel1, Relation rel2) {
        validateCompatibility(rel1, rel2);
        Relation union = new RelationBuilder()
                .attributeNames(rel1.getAttrs())
                .attributeTypes(rel1.getTypes())
                .build();

        Set<List<Cell>> addedRows = new HashSet<>();
        int counter = 0;

        // Add rows from the first relation
        for (int i = 0; i < rel1.getSize() && counter < ROW_LIMIT; i++) {
            List<Cell> row = rel1.getRow(i);
            if (addedRows.add(row)) {
                union.insert(row);
                counter++;
            }
        }

        // Add rows from the second relation
        for (int i = 0; i < rel2.getSize() && counter < ROW_LIMIT; i++) {
            List<Cell> row = rel2.getRow(i);
            if (addedRows.add(row)) {
                union.insert(row);
                counter++;
            }
        }

        return union;
    }

    /**
     * Perform the set difference operation on the relations rel1 and rel2
     * 
     * @param rel1 The first relation.
     * @param rel2 The second relation.
     * @return The resulting relation after applying the set difference operation.
     * @throws IllegalArgumentException If rel1 and rel2 are not compatible.
     */
    @Override
    public Relation diff(Relation rel1, Relation rel2) {
        validateCompatibility(rel1, rel2);
        Relation diff = new RelationBuilder()
                .attributeNames(rel1.getAttrs())
                .attributeTypes(rel1.getTypes())
                .build();

        Set<List<Cell>> rel2Rows = new HashSet<>();
        for (int i = 0; i < rel2.getSize(); i++) {
            rel2Rows.add(rel2.getRow(i));
        }

        int counter = 0;
        for (int i = 0; i < rel1.getSize() && counter < ROW_LIMIT; i++) {
            List<Cell> row = rel1.getRow(i);
            if (!rel2Rows.contains(row)) {
                diff.insert(row);
                counter++;
            }
        }

        return diff;
    }

    /**
     * Rename the attributes in origAttr of relation rel to corresponding
     * names in renamedAttr
     * 
     * @param rel         The relation to be renamed.
     * @param origAttr    The original attribute names.
     * @param renamedAttr The new attribute names.
     * @return The resulting relation after renaming the attributes.
     * @throws IllegalArgumentException If attributes in origAttr are not present in
     */
    @Override
    public Relation rename(Relation rel, List<String> origAttr, List<String> renamedAttr) {
        if (origAttr.size() != renamedAttr.size()) {
            throw new IllegalArgumentException("Original and renamed attribute lists must be of the same size.");
        }

        List<Type> types = new ArrayList<>();
        List<String> newAttrNames = new ArrayList<>(rel.getAttrs());
        for (int i = 0; i < origAttr.size(); i++) {
            int index = newAttrNames.indexOf(origAttr.get(i));
            if (index == -1) {
                throw new IllegalArgumentException("Attribute not found: " + origAttr.get(i));
            }
            newAttrNames.set(index, renamedAttr.get(i));
            types.add(rel.getTypes().get(index));
        }

        Relation renamedRelation = new RelationBuilder()
                .attributeNames(newAttrNames)
                .attributeTypes(types)
                .build();

        int counter = 0;
        for (int i = 0; i < rel.getSize() && counter < ROW_LIMIT; i++) {
            renamedRelation.insert(rel.getRow(i));
            counter++;
        }

        return renamedRelation;
    }

    /**
     * Perform cartisian product on relations rel1 and rel2
     * 
     * @param rel1 The first relation.
     * @param rel2 The second relation.
     * @return The resulting relation after applying cartisian product.
     * @throws IllegalArgumentException if rel1 and rel2 have common attibutes.
     * 
     */
    @Override
    public Relation cartesianProduct(Relation rel1, Relation rel2) {
        List<String> newAttrNames = new ArrayList<>(rel1.getAttrs());
        newAttrNames.addAll(rel2.getAttrs());

        List<Type> newTypes = new ArrayList<>(rel1.getTypes());
        newTypes.addAll(rel2.getTypes());

        Relation cartesian = new RelationBuilder()
                .attributeNames(newAttrNames)
                .attributeTypes(newTypes)
                .build();

        int counter = 0;
        for (int i = 0; i < rel1.getSize() && counter < ROW_LIMIT; i++) {
            for (int j = 0; j < rel2.getSize() && counter < ROW_LIMIT; j++) {
                List<Cell> newRow = new ArrayList<>(rel1.getRow(i));
                newRow.addAll(rel2.getRow(j));
                cartesian.insert(newRow);
                counter++;
            }
        }

        return cartesian;
    }

    /**
     * Perform natural join on relations rel1 and rel2
     * 
     * @param rel1 The first relation.
     * @param rel2 The second relation.
     * @return The resulting relation after applying natural join.
     *
     * @throws IllegalArgumentException If rel1 and rel2 are not compatible.
     */

    @Override
    public Relation join(Relation rel1, Relation rel2) {
        // Find common attributes
        List<String> commonAttrs = rel1.getAttrs().stream()
                .filter(rel2.getAttrs()::contains)
                .collect(Collectors.toList());

        if (commonAttrs.isEmpty()) {
            throw new IllegalArgumentException("No common attributes for natural join.");
        }

        // Prepare new attributes and types for the resulting relation
        List<String> newAttrs = new ArrayList<>(rel1.getAttrs());
        List<Type> newTypes = new ArrayList<>(rel1.getTypes());

        for (String attr : rel2.getAttrs()) {
            if (!commonAttrs.contains(attr)) {
                newAttrs.add(attr);
                newTypes.add(rel2.getTypes().get(rel2.getAttrs().indexOf(attr)));
            }
        }

        Relation join = new RelationBuilder()
                .attributeNames(newAttrs)
                .attributeTypes(newTypes)
                .build();

        int counter = 0;
        for (int i = 0; i < rel1.getSize() && counter < ROW_LIMIT; i++) {
            for (int j = 0; j < rel2.getSize() && counter < ROW_LIMIT; j++) {
                List<Cell> row1 = rel1.getRow(i);
                List<Cell> row2 = rel2.getRow(j);

                if (rowsMatchOnCommonAttributes(row1, row2, rel1, rel2, commonAttrs)) {
                    List<Cell> newRow = new ArrayList<>(row1);
                    for (String attr : rel2.getAttrs()) {
                        if (!commonAttrs.contains(attr)) {
                            newRow.add(row2.get(rel2.getAttrs().indexOf(attr)));
                        }
                    }
                    join.insert(newRow);
                    counter++;
                }
            }
        }

        return join;
    }

    /**
     * Perform theta join on relations rel1 and rel2
     *
     * @param rel1 The first relation.
     * @param rel2 The second relation.
     * @return The resulting relation after applying natural join.
     *
     * @throws IllegalArgumentException If rel1 and rel2 are not compatible.
     */
    @Override
    public Relation join(Relation rel1, Relation rel2, Predicate p) {
        // Join based on a predicate
        List<String> newAttrNames = new ArrayList<>(rel1.getAttrs());
        newAttrNames.addAll(rel2.getAttrs());

        List<Type> newTypes = new ArrayList<>(rel1.getTypes());
        newTypes.addAll(rel2.getTypes());

        Relation join = new RelationBuilder()
                .attributeNames(newAttrNames)
                .attributeTypes(newTypes)
                .build();

        int counter = 0;
        for (int i = 0; i < rel1.getSize() && counter < ROW_LIMIT; i++) {
            for (int j = 0; j < rel2.getSize() && counter < ROW_LIMIT; j++) {
                List<Cell> newRow = new ArrayList<>(rel1.getRow(i));
                newRow.addAll(rel2.getRow(j));
                if (p.check(newRow)) {
                    join.insert(newRow);
                    counter++;
                }
            }
        }

        return join;
    }

    // Utility method to check if two rows match on common attributes
    private boolean rowsMatchOnCommonAttributes(List<Cell> row1, List<Cell> row2, Relation rel1, Relation rel2,
            List<String> commonAttrs) {
        for (String attr : commonAttrs) {
            int index1 = rel1.getAttrs().indexOf(attr);
            int index2 = rel2.getAttrs().indexOf(attr);
            if (!row1.get(index1).equals(row2.get(index2))) {
                return false;
            }
        }
        return true;
    }

    // Utility method to validate compatibility of two relations for union and diff
    // operations
    private void validateCompatibility(Relation rel1, Relation rel2) {
        if (!rel1.getAttrs().equals(rel2.getAttrs()) || !rel1.getTypes().equals(rel2.getTypes())) {
            throw new IllegalArgumentException("Relations are not compatible for operation.");
        }
    }
}
