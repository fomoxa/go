package shared

//cyclone:model codec=edge
type Player struct {
	HP   uint32 `cyclone:"u32" codec:"edge"`
	Name string `cyclone:"string" codec:"edge"`
}
