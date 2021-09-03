
/**
 * 测试数据
 *
 * @author YI
 * @date create in 2021/9/2 18:39
 */
public class TestModel {
    int app_id;
    Integer svip_level;
    Long svip_remain;
    String name;
    double money;

    public TestModel(int app_id, Integer svip_level, Long svip_remain, String name, double money) {
        this.app_id = app_id;
        this.svip_level = svip_level;
        this.svip_remain = svip_remain;
        this.name = name;
        this.money = money;
    }

    public int getApp_id() {
        return app_id;
    }

    public void setApp_id(int app_id) {
        this.app_id = app_id;
    }

    public Integer getSvip_level() {
        return svip_level;
    }

    public void setSvip_level(Integer svip_level) {
        this.svip_level = svip_level;
    }

    public Long getSvip_remain() {
        return svip_remain;
    }

    public void setSvip_remain(Long svip_remain) {
        this.svip_remain = svip_remain;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getMoney() {
        return money;
    }

    public void setMoney(double money) {
        this.money = money;
    }

    @Override
    public String toString() {
        return "TestModel{" +
                "app_id=" + app_id +
                ", svip_level=" + svip_level +
                ", svip_remain=" + svip_remain +
                ", name='" + name + '\'' +
                ", money=" + money +
                '}';
    }
}
