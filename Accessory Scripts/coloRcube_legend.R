##  Creates a 3D color cube to be used for understanding the RGB raster of 
##  global relative inequalities.
require(plotly)
require(htmlwidgets)
##  Create an expanded dataframe of the cube we are going to create with 
##  dimensions x, y, and z; 6 cubes on each axis:
grd <- expand.grid(Population=seq(0,1,0.2), 
                   Urban=seq(0,1,0.2), 
                   Lights=seq(0,1,0.2))
##  Create the colors:
grd$col <- rgb(red=sqrt(grd$Population^2),
               green=sqrt(grd$Urban^2),
               blue=sqrt(grd$Lights^2))
#alpha=0.1)
# maxColorValue = 255)

fig <- plot_ly(grd,
               x=~Population,
               y=~Urban,
               z=~Lights, 
               marker =list(symbol="square", color=grd$col),
               showlegend=F) %>%
  add_markers() %>%
  layout(plot_bgcolor="black",
         paper_bgcolor="black",
         scene=list(xaxis = list(title="Population",color="white"),
                    yaxis = list(title="Urban",color="white"),
                    zaxis = list(title="Lights",color="white")))

fig

saveWidget(fig, file = "E:/Research/GRI_color_cube_legend.html",selfcontained = T)
