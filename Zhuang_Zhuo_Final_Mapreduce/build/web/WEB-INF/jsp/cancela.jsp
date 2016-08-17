<%-- 
    Document   : cancela
    Created on : Dec 16, 2015, 1:41:01 AM
    Author     : Zhuang Zhuo <zhuo.z@husky.neu.edu>
--%>
<html>
<head></head>
<body>

<div id="pieChart"></div>

<script src="//cdnjs.cloudflare.com/ajax/libs/d3/3.4.4/d3.min.js"></script>
<script src="https://rawgit.com/benkeen/d3pie/0.1.8/d3pie/d3pie.min.js"></script>
<script>
var pie = new d3pie("pieChart", {
	"header": {
		"title": {
			"text": "\tCancellation",
			"fontSize": 24,
			"font": "open sans"
		},
		"subtitle": {
			"text": "The biggest reason and the amount of  cancellation in every month",
			"color": "#999999",
			"fontSize": 12,
			"font": "open sans"
		},
		"titleSubtitlePadding": 9
	},
	"footer": {
		"color": "#999999",
		"fontSize": 10,
		"font": "open sans",
		"location": "bottom-left"
	},
	"size": {
		"canvasWidth": 590,
		"pieOuterRadius": "90%"
	},
	"data": {
		"sortOrder": "value-desc",
		"content": [
			{
				"label": "Jan(carrier)",
				"value": 4541,
				"color": "#4d53f0"
			},
			{
				"label": "Feb(weather)",
				"value": 6202,
				"color": "#1d1f39"
			},
			{
				"label": "Mar(carrier)",
				"value": 4100,
				"color": "#2f52aa"
			},
			{
				"label": "Apr(carrier)",
				"value": 3844,
				"color": "#2e58c9"
			},
			{
				"label": "May(carrier)",
				"value": 3617,
				"color": "#4d6fcc"
			},
			{
				"label": "Jun(carrier)",
				"value": 5475,
				"color": "#738def"
			},
			{
				"label": "Jul(carrier)",
				"value": 5552,
				"color": "#3a81e9"
			},
			{
				"label": "Aug(carrier)",
				"value": 5529,
				"color": "#3366e6"
			},
			{
				"label": "Sep(carrier)",
				"value": 5452,
				"color": "#417be6"
			},
			{
				"label": "Oct(carrier)",
				"value": 5042,
				"color": "#3480e1"
			},
			{
				"label": "Nov(carrier)",
				"value": 3743,
				"color": "#3944d2"
			},
			{
				"label": "Dec(weather)",
				"value": 10016,
				"color": "#000000"
			}
		]
	},
	"labels": {
		"outer": {
			"pieDistance": 15
		},
		"inner": {
			"format": "value"
		},
		"mainLabel": {
			"fontSize": 11
		},
		"percentage": {
			"color": "#ffffff",
			"decimalPlaces": 0
		},
		"value": {
			"color": "#adadad",
			"fontSize": 11
		},
		"lines": {
			"enabled": true
		},
		"truncation": {
			"enabled": true
		}
	},
	"effects": {
		"pullOutSegmentOnClick": {
			"effect": "linear",
			"speed": 400,
			"size": 8
		}
	},
	"misc": {
		"gradient": {
			"enabled": true,
			"percentage": 100
		}
	}
});
</script>

</body>
</html>
