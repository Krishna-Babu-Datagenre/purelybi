import { Widget } from '../types';

export const dummyWidgets: Widget[] = [
  {
    id: 'widget-1',
    title: 'Sales by Month',
    type: 'bar',
    chartConfig: {
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#1e293b',
        borderColor: 'transparent',
        textStyle: { color: '#f1f5f9', fontSize: 12 },
        axisPointer: { type: 'shadow' },
      },
      grid: {
        top: 16,
        right: 16,
        bottom: 8,
        left: 8,
        containLabel: true,
      },
      xAxis: {
        type: 'category',
        data: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        axisLine: { lineStyle: { color: '#e2e8f0' } },
        axisTick: { show: false },
        axisLabel: { color: '#94a3b8', fontSize: 11 },
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          color: '#94a3b8',
          fontSize: 11,
          formatter: (v: number) => `$${(v / 1000).toFixed(0)}k`,
        },
        splitLine: { lineStyle: { color: '#f1f5f9', type: 'dashed' } },
        axisLine: { show: false },
        axisTick: { show: false },
      },
      series: [
        {
          name: 'Sales',
          type: 'bar',
          barMaxWidth: 40,
          data: [12000, 15000, 9800, 17500, 21000, 18400, 23000, 19500, 16000, 22000, 25000, 28000],
          itemStyle: {
            color: {
              type: 'linear',
              x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: '#818cf8' },
                { offset: 1, color: '#6366f1' },
              ],
            },
            borderRadius: [4, 4, 0, 0],
          },
          emphasis: { itemStyle: { color: '#4f46e5' } },
        },
      ],
    },
  },
  {
    id: 'widget-2',
    title: 'Revenue by Region',
    type: 'pie',
    chartConfig: {
      tooltip: {
        trigger: 'item',
        backgroundColor: '#1e293b',
        borderColor: 'transparent',
        textStyle: { color: '#f1f5f9', fontSize: 12 },
        formatter: '{b}<br/>Revenue: <b>${c}</b> ({d}%)',
      },
      legend: {
        orient: 'horizontal',
        bottom: 4,
        left: 'center',
        itemWidth: 10,
        itemHeight: 10,
        borderRadius: 2,
        textStyle: { color: '#64748b', fontSize: 11 },
      },
      series: [
        {
          name: 'Revenue',
          type: 'pie',
          radius: ['38%', '65%'],
          center: ['50%', '45%'],
          avoidLabelOverlap: true,
          label: {
            show: false,
          },
          emphasis: {
            label: { show: false },
            itemStyle: {
              shadowBlur: 12,
              shadowOffsetX: 0,
              shadowColor: 'rgba(0,0,0,0.2)',
            },
          },
          data: [
            { value: 48000, name: 'North America', itemStyle: { color: '#6366f1' } },
            { value: 32000, name: 'Europe', itemStyle: { color: '#8b5cf6' } },
            { value: 21000, name: 'Asia Pacific', itemStyle: { color: '#06b6d4' } },
            { value: 11000, name: 'Latin America', itemStyle: { color: '#10b981' } },
            { value: 6000, name: 'Middle East & Africa', itemStyle: { color: '#f59e0b' } },
          ],
        },
      ],
    },
  },
  {
    id: 'widget-3',
    title: 'Total Revenue',
    type: 'kpi',
    chartConfig: {
      value: 118000,
      prefix: '$',
      change: 12.5,
      changeLabel: 'vs last month',
      icon: 'revenue',
    },
  },
  {
    id: 'widget-4',
    title: 'Total Orders',
    type: 'kpi',
    chartConfig: {
      value: 4821,
      change: 8.1,
      changeLabel: 'vs last month',
      icon: 'orders',
    },
  },
  {
    id: 'widget-5',
    title: 'Avg. Order Value',
    type: 'kpi',
    chartConfig: {
      value: 244,
      prefix: '$',
      change: -3.2,
      changeLabel: 'vs last month',
      icon: 'aov',
    },
  },
  {
    id: 'widget-6',
    title: 'New Customers',
    type: 'kpi',
    chartConfig: {
      value: 1340,
      change: 21.0,
      changeLabel: 'vs last month',
      icon: 'customers',
    },
  },
  {
    id: 'widget-7',
    title: 'Monthly Active Users',
    type: 'bar',
    chartConfig: {
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#1e293b',
        borderColor: 'transparent',
        textStyle: { color: '#f1f5f9', fontSize: 12 },
        axisPointer: { type: 'shadow' },
      },
      grid: {
        top: 16,
        right: 16,
        bottom: 8,
        left: 8,
        containLabel: true,
      },
      legend: {
        top: 0,
        right: 0,
        itemWidth: 10,
        itemHeight: 10,
        textStyle: { color: '#64748b', fontSize: 11 },
      },
      xAxis: {
        type: 'category',
        data: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        axisLine: { lineStyle: { color: '#e2e8f0' } },
        axisTick: { show: false },
        axisLabel: { color: '#94a3b8', fontSize: 11 },
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          color: '#94a3b8',
          fontSize: 11,
          formatter: (v: number) => `${(v / 1000).toFixed(0)}k`,
        },
        splitLine: { lineStyle: { color: '#f1f5f9', type: 'dashed' } },
        axisLine: { show: false },
        axisTick: { show: false },
      },
      series: [
        {
          name: 'New Users',
          type: 'bar',
          stack: 'users',
          barMaxWidth: 32,
          data: [3200, 4100, 3800, 5200, 6100, 5800, 7200, 6900, 5400, 7800, 8500, 9200],
          itemStyle: {
            color: '#06b6d4',
            borderRadius: [0, 0, 0, 0],
          },
        },
        {
          name: 'Returning',
          type: 'bar',
          stack: 'users',
          barMaxWidth: 32,
          data: [8800, 9400, 8200, 10500, 11900, 10200, 13100, 12500, 10800, 14200, 15800, 17400],
          itemStyle: {
            color: '#6366f1',
            borderRadius: [4, 4, 0, 0],
          },
        },
      ],
    },
  },
];

