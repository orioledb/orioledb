thread apply all bt full
up 99999
set $i=0
set $end=argc
while ($i < $end)
p argv[$i++]
end
quit
