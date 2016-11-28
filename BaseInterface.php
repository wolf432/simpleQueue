<?
namespace polar\components\simpleQueue;

/**
 * 队列的基础操作接口
 * @author linan<linan@51cto.com>
 * @date    2016-08-17
 */
interface BaseInterface
{
    /**
     * 写入数据
     * 
     * @param string $name      队列名
     * @param string $data      数据
     * @param int $priority     队列的优先级
     * @access public
     * @throw   如果加入队列失败会抛出异常
     */
    public function push($name = '', $data = '', $priority = 0);

    /**
     * 取出数据
     * 
     * @param string $name  队列名
     * @access public
     * @return String
     */
    public function pull($name = '');
}
