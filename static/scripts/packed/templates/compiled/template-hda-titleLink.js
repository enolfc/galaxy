(function(){var b=Handlebars.template,a=Handlebars.templates=Handlebars.templates||{};a["template-hda-titleLink"]=b(function(e,k,d,j,i){this.compilerInfo=[4,">= 1.0.0"];d=this.merge(d,e.helpers);i=i||{};var g="",c,f="function",h=this.escapeExpression;g+='<span class="historyItemTitle">';if(c=d.hid){c=c.call(k,{hash:{},data:i})}else{c=k.hid;c=typeof c===f?c.apply(k):c}g+=h(c)+": ";if(c=d.name){c=c.call(k,{hash:{},data:i})}else{c=k.name;c=typeof c===f?c.apply(k):c}g+=h(c)+"</span>";return g})})();