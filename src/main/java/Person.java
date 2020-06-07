public class Person {

    private int id;
    private String name;
    private int age;
    private String sex;
    private String region;
    private String status;

    public Person(String name, String sex, int age, String region, String status) {

        this.name = name;
        this.age = age;
        this.sex = sex;
        this.region = region;
        this.status = status;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getSex() {
        return sex;
    }

    public String getRegion() {
        return region;
    }

    public String getStatus() {
        return status;
    }
}
