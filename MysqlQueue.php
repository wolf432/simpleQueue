<?php

namespace polar\components\simpleQueue;

use yii\db\Connection;
use yii\di\Instance;

/**
 * 使用Mysql做为队列的存储服务建议只在简单的业务逻辑下使用。
 *
 * 在yii配置文件中加载组件即可开启服务
 *
 * 配置方法
 * db为数据库组件的组件id，如果没有设置数据库组件先创建一个在使用该组件
 * table_name 为保存队列数据的数据库表名
 *
 *  'queue' => [
 *       class' => 'polar\components\simpleQueue\MysqlQueue',
 *          'db' => 'db',
 *          'table_name' => 'queue'
 *       ]
 *   ]
 *
 * 数据库表结构
 * CREATE TABLE `queue` (
 *   `qid` int(11) unsigned NOT NULL AUTO_INCREMENT,
 *   `name` varchar(20) NOT NULL DEFAULT '' COMMENT '队列名',
 *   `priority` tinyint(4) unsigned NOT NULL COMMENT '优先级,1为低级,2为中级,3为高级',
 *   `data` varchar(500) NOT NULL DEFAULT '' COMMENT '数据',
 *   `add_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入时间',
 *   `status` tinyint(3) unsigned DEFAULT '0' COMMENT '0为等待取数据,1为取出数据完成',
 *   PRIMARY KEY (`qid`)
 * ) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;
 * 
 *
 * @author linan<linan@51cto.com>
 */
class MysqlQueue extends \yii\base\Component implements BaseInterface 
{
    /*
     * 队列中的等级值
     */
    const PRI_HEIGHT    = 3;
    const PRI_MIDDLE    = 2;
    const PRI_LOW       = 1;

    /*
     * 数据处理状态
     */
    const STATUS_WAIT       = 0;
    const STATIS_DONE       = 1;

    /**
     * 用来保存队列数据的数据表
     * 
     * @var string
     * @access public
     */
    public $table_name = '';

    /**
     * 数据库组件id
     * 
     * @var string
     * @access public
     */
    public $db = 'db';

    /**
     * 队列的等级数组
     * 
     * @var int 
     * @access private
     */
    private $priority_arr = [3,2,1];

    /*
     * 获取数据库实例
     */
    public function init()
    {/*{{{*/
        parent::init();
        $this->db = Instance::ensure($this->db, Connection::className());
    }/*}}}*/

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
            throw new \Exception("队列名不能为空", -101);

        if(!in_array($priority, $this->priority_arr))
            throw new \Exception("不支持的优先级",-102);

        $this->db->createCommand()->insert($this->table_name, [
            'name'      => $name,
            'data'      => $data,
            'status'    => 0,
            'priority'  => $priority,
        ])->execute();
    }/*}}}*/

    /**
     * 从队列中取出一条数据
     * 
     * @param string $name  队列名
     * @access public
     * @return String
     */
    public function pull($name = '')
    {/*{{{*/
        $lock_data_sql = "update {$this->table_name} set qid=LAST_INSERT_ID(qid), status=".self::STATIS_DONE." where status=".self::STATUS_WAIT." order by priority desc limit 1";
        $fetch_data_sql = "select data from {$this->table_name} where qid=last_insert_id() and row_count() > 0"; 

        $this->db->createCommand($lock_data_sql)->execute();
        $data = $this->db->createCommand($fetch_data_sql)->queryOne();

        return empty($data)? '':$data['data'];
    }/*}}}*/
}
