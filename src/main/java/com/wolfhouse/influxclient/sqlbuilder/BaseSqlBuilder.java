package com.wolfhouse.influxclient.sqlbuilder;

/**
 * SQL 构造器模板类，规范 SQL 构建的方法步骤
 *
 * @author Rylin Wolf
 */
public abstract class BaseSqlBuilder {
    /**
     * 构建查询目标部分
     *
     * @param builder 构建目标，围绕该 builder 进行拼接
     */
    protected abstract void buildTarget(StringBuilder builder);

    /**
     * 构建查询目标表
     *
     * @param builder 构建目标，围绕该 builder 进行拼接
     */
    protected abstract void buildFromTable(StringBuilder builder);

    /**
     * 构建查询条件
     *
     * @param builder 构建目标，围绕该 builder 进行拼接
     */
    protected void buildCondition(StringBuilder builder) {}

    /**
     * 构建查询修饰符(order by, limit 等)
     *
     * @param builder 构建目标，围绕该 builder 进行拼接
     */
    protected void buildModifies(StringBuilder builder) {}

    /**
     * 执行构建前的字段验证
     *
     * @return 是否验证成功
     */
    protected abstract boolean validate();

    /**
     * 执行构建，步骤如下：
     * 1. 构建前字段验证
     * 2. 构建查询目标
     * 3. 构建目标表
     * 4. 构建查询条件
     */
    public String build() {
        if (!validate()) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        buildTarget(builder);
        buildFromTable(builder);
        buildCondition(builder);
        buildModifies(builder);
        return builder.toString().trim();
    }
}
