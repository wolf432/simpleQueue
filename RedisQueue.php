<?php

namespace yii\simpleQueue;

use yii\db\Connection;
use yii\di\Instance;

/**
 * 使用Redis作为队列的存储服务,使用集合来记录程序中开启了几个队列，可以用来统计队列的运行情况
 *
 * 注意不要开启序列化的选项，在删除统计集合的元素时候回出现删除失败的问题
 * Redis::OPT_SERIALIZER =>\Redis::SERIALIZER_PHP
 *
 * 配置方法
 * db为Redis库组件的组件id，如果没有设置Redis库组件先创建一个在使用该组件
 * di_class 为Redis组件用到的类文件的类名
 * table_name 为统计队列的集合名字，用来区分不同应用的队列系统
 *
 *  'queue' => [
 *       class' => 'polar\components\simpleQueue\RedisQueue',
 *          'db'        => 'db',
 *          'di_class'  => 'polar\base\redis\PhpRedis',
 *          'table_name'=> 'queue',
 *       ]
 *   ]
 * @author linan<linan731@gmail.com>
 */
class RedisQueue extends \yii\base\Component implements BaseInterface
{
    /**
     * Redis组件id
     * 
     * @var mixed
     * @access public
     */
    public $db          = 'redis';
    public $di_class    = 'polar\base\redis\PhpRedis';
    public $table_name  = 'queue';

    public function init()
    {
        $this->db = Instance::ensure($this->db , $this->di_class);
    }

    /*
     * 队列中的等级值
     */
    const PRI_HEIGHT    = 3;
    const PRI_MIDDLE    = 2;
    const PRI_LOW       = 1;

    /**
     * 队列的等级数组
     * 
     * @var int 
     * @access private
     */
    private $priority_arr = [3,2,1];

    /**
     * 加入数据到队列中
     * 
     * @param string    $name       队列名
     * @param string    $data       数据
     * @param int       $priority   队列等级，默认为低等级
     * @access public
     * @throw   如果加入队列失败会抛出异常
     */
    public function push($name = '', $data = '', $priority = self::PRI_LOW)
    {/*{{{*/
        if(empty($name))
            throw new \Exception("队列名不能为空",-101);

        if(!in_array($priority, $this->priority_arr))
            throw new \Exception("不支持的优先级",-102);

        $queue_name = $name.'_'.$priority;

        $rs = $this->db->lpush($queue_name, $data);
        if($rs == false)
            throw new \Exception("push失败", -103);

        $this->db->sAdd($this->table_name, $queue_name);
    }/*}}}*/

    public function pull($name ='')
    {/*{{{*/
        foreach($this->priority_arr as $pr)
        {
            $queue_name = $name.'_'.$pr;
            $data = $this->db->lpop($queue_name);
            //如果队列已经没有数据了则删除统计集合集合中的队列名
            if($this->db->llen($queue_name) == 0)
                $this->db->sRem($this->table_name, $queue_name);

            if($data != false)
                break;
        }
        return ($data == false)? '':$data;
    }/*}}}*/

}
